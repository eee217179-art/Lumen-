"""
LumenКонструктор — Telegram-бот-конструктор ботов (aiogram 3 + aiosqlite).

Как работает:
  1. Пользователь пишет главному боту /start и жмёт «Добавить бота».
  2. Кидает токен от @BotFather — конструктор поднимает этого бота.
  3. Через меню настраивает: приветствие, команды, кнопки, ответ по умолчанию,
     рассылку, статистику, вкл/выкл.

Запуск:
  pip install -r requirements.txt
  export MAIN_BOT_TOKEN="токен_главного_бота"   (Windows: set MAIN_BOT_TOKEN=...)
  python main.py
"""
import asyncio
import html
import logging
import os
import re

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramUnauthorizedError,
)
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "constructor.db")
MAX_BOTS_PER_USER = 5
MAX_COMMANDS_PER_BOT = 50
MAX_BUTTONS_PER_COMMAND = 8

TOKEN_RE = re.compile(r"^\d{6,12}:[A-Za-z0-9_-]{30,}$")
CMD_RE = re.compile(r"^[a-z0-9_]{1,32}$")

db: aiosqlite.Connection
# bot_id -> (Bot, Dispatcher, Task)
running: dict[int, tuple[Bot, Dispatcher, asyncio.Task]] = {}


# ───────────────────────── База данных ─────────────────────────
async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA foreign_keys = ON")
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS bots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            username TEXT,
            name TEXT,
            fallback TEXT,
            active INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS commands (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id INTEGER NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            text TEXT NOT NULL,
            UNIQUE(bot_id, name)
        );
        CREATE TABLE IF NOT EXISTS buttons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            command_id INTEGER NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
            text TEXT NOT NULL,
            kind TEXT NOT NULL,          -- 'url' | 'cmd'
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS child_users (
            bot_id INTEGER NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id)
        );
        """
    )
    await db.commit()


async def fetch(sql, *args):
    async with db.execute(sql, args) as cur:
        return await cur.fetchall()


async def one(sql, *args):
    rows = await fetch(sql, *args)
    return rows[0] if rows else None


async def run(sql, *args):
    cur = await db.execute(sql, args)
    await db.commit()
    return cur.lastrowid


# ───────────────────────── Дочерние боты ─────────────────────────
async def build_markup(command_id: int) -> InlineKeyboardMarkup | None:
    rows = await fetch("SELECT * FROM buttons WHERE command_id=? ORDER BY id", command_id)
    if not rows:
        return None
    btns = []
    for b in rows:
        if b["kind"] == "url":
            btns.append(InlineKeyboardButton(text=b["text"], url=b["value"]))
        else:
            btns.append(InlineKeyboardButton(text=b["text"], callback_data=f"c:{b['value']}"))
    return InlineKeyboardMarkup(inline_keyboard=[btns[i:i + 2] for i in range(0, len(btns), 2)])


def make_child_dp(bot_id: int) -> Dispatcher:
    dp = Dispatcher()

    async def touch(user_id: int):
        await run("INSERT OR IGNORE INTO child_users(bot_id, user_id) VALUES(?,?)", bot_id, user_id)

    @dp.message()
    async def on_message(m: Message):
        if m.from_user:
            await touch(m.from_user.id)
        text = m.text or ""
        if text.startswith("/"):
            name = text[1:].split()[0].split("@")[0].lower() if len(text) > 1 else ""
            cmd = await one("SELECT * FROM commands WHERE bot_id=? AND name=?", bot_id, name)
            if cmd:
                await m.answer(cmd["text"], reply_markup=await build_markup(cmd["id"]))
                return
        bot_row = await one("SELECT fallback FROM bots WHERE id=?", bot_id)
        if bot_row and bot_row["fallback"]:
            await m.answer(bot_row["fallback"])

    @dp.callback_query(F.data.startswith("c:"))
    async def on_callback(c: CallbackQuery):
        cid = int(c.data.split(":")[1])
        cmd = await one("SELECT * FROM commands WHERE id=? AND bot_id=?", cid, bot_id)
        if not cmd:
            await c.answer("Эта команда удалена", show_alert=True)
            return
        await c.answer()
        if c.message:
            await c.message.answer(cmd["text"], reply_markup=await build_markup(cmd["id"]))

    return dp


async def _run_child(bot_id: int, bot: Bot, dp: Dispatcher):
    try:
        await dp.start_polling(bot, handle_signals=False)
    except TelegramUnauthorizedError:
        logging.warning("Токен бота %s отозван — отключаю", bot_id)
        await run("UPDATE bots SET active=0 WHERE id=?", bot_id)
        running.pop(bot_id, None)
    except asyncio.CancelledError:
        raise
    except Exception:
        logging.exception("Ошибка в дочернем боте %s", bot_id)


async def start_child(bot_id: int, token: str):
    if bot_id in running:
        return
    bot = Bot(token)
    dp = make_child_dp(bot_id)
    task = asyncio.create_task(_run_child(bot_id, bot, dp))
    running[bot_id] = (bot, dp, task)
    await sync_commands(bot_id)


async def stop_child(bot_id: int):
    item = running.pop(bot_id, None)
    if not item:
        return
    bot, dp, task = item
    try:
        await dp.stop_polling()
    except Exception:
        pass
    task.cancel()
    try:
        await bot.session.close()
    except Exception:
        pass


async def sync_commands(bot_id: int):
    """Обновляет меню команд (синяя кнопка «Меню») у дочернего бота."""
    item = running.get(bot_id)
    if not item:
        return
    rows = await fetch("SELECT name, text FROM commands WHERE bot_id=? ORDER BY id LIMIT 100", bot_id)
    cmds = [
        BotCommand(command=r["name"], description=(r["text"].replace("\n", " ")[:50] or r["name"]))
        for r in rows
    ]
    try:
        await item[0].set_my_commands(cmds)
    except Exception:
        logging.exception("set_my_commands failed")


# ───────────────────────── Главный бот: UI ─────────────────────────
def kb(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=t, callback_data=d) for t, d in row] for row in rows]
    )


MENU_KB = kb([("➕ Добавить бота", "add")], [("🤖 Мои боты", "bots")])
CANCEL_KB = kb([("✖️ Отмена", "menu")])

WELCOME = (
    "✨ <b>LumenКонструктор</b>\n\n"
    "Создай своего Telegram-бота без кода: кидаешь токен от @BotFather — "
    "и настраиваешь команды, кнопки, приветствие и рассылки прямо здесь."
)


class AddBot(StatesGroup):
    token = State()


class AddCmd(StatesGroup):
    name = State()
    text = State()


class EditText(StatesGroup):
    text = State()


class AddBtn(StatesGroup):
    text = State()
    target = State()


class SetFallback(StatesGroup):
    text = State()


class Broadcast(StatesGroup):
    text = State()


async def edit(c: CallbackQuery, text: str, markup: InlineKeyboardMarkup | None = None):
    try:
        await c.message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest as e:
        if "not modified" not in str(e):
            await c.message.answer(text, reply_markup=markup)
    await c.answer()


async def own_bot(user_id: int, bot_id: int):
    return await one("SELECT * FROM bots WHERE id=? AND owner_id=?", bot_id, user_id)


async def own_cmd(user_id: int, cid: int):
    return await one(
        "SELECT c.* FROM commands c JOIN bots b ON b.id=c.bot_id WHERE c.id=? AND b.owner_id=?",
        cid, user_id,
    )


async def bot_panel(bot_id: int):
    b = await one("SELECT * FROM bots WHERE id=?", bot_id)
    n_cmd = (await one("SELECT COUNT(*) n FROM commands WHERE bot_id=?", bot_id))["n"]
    n_usr = (await one("SELECT COUNT(*) n FROM child_users WHERE bot_id=?", bot_id))["n"]
    start = await one("SELECT id FROM commands WHERE bot_id=? AND name='start'", bot_id)
    status = "🟢 работает" if b["active"] else "🔴 выключен"
    text = (
        f"🤖 <b>{html.escape(b['name'] or '')}</b> @{html.escape(b['username'] or '')}\n"
        f"Статус: {status}\n"
        f"Команд: {n_cmd} · Пользователей: {n_usr}"
    )
    rows = []
    if start:
        rows.append([("👋 Приветствие (/start)", f"bc:{start['id']}")])
    rows += [
        [("📋 Команды", f"bcs:{bot_id}"), ("💬 Ответ по умолчанию", f"bfb:{bot_id}")],
        [("📣 Рассылка", f"bbr:{bot_id}"), ("📊 Статистика", f"bst:{bot_id}")],
        [("⏸ Выключить" if b["active"] else "▶️ Включить", f"btg:{bot_id}")],
        [("🗑 Удалить бота", f"bdel:{bot_id}")],
        [("⬅️ Назад", "bots")],
    ]
    return text, kb(*rows)


async def cmd_panel(cid: int):
    c = await one("SELECT * FROM commands WHERE id=?", cid)
    btns = await fetch("SELECT * FROM buttons WHERE command_id=? ORDER BY id", cid)
    label = "приветствие /start" if c["name"] == "start" else f"/{c['name']}"
    preview = html.escape(c["text"][:400]) + ("…" if len(c["text"]) > 400 else "")
    text = f"⚙️ <b>{label}</b>\n\n{preview}\n\n"
    if btns:
        text += "<b>Кнопки:</b>\n" + "\n".join(
            f"• {html.escape(b['text'])} → " + (html.escape(b["value"]) if b["kind"] == "url" else "команда")
            for b in btns
        )
    else:
        text += "Кнопок пока нет."
    rows = [
        [("✏️ Изменить текст", f"cet:{cid}")],
        [("➕ Кнопка", f"cba:{cid}")],
    ]
    if btns:
        rows.append([("❌ Удалить кнопку", f"cbd:{cid}")])
    if c["name"] != "start":
        rows.append([("🗑 Удалить команду", f"cdel:{cid}")])
    rows.append([("⬅️ Назад", f"b:{c['bot_id']}" if c["name"] == "start" else f"bcs:{c['bot_id']}")])
    return text, kb(*rows)


# ───────────────────────── Главный бот: хендлеры ─────────────────────────
def register(dp: Dispatcher):
    @dp.message(CommandStart())
    async def start(m: Message, state: FSMContext):
        await state.clear()
        await m.answer(WELCOME, reply_markup=MENU_KB)

    @dp.callback_query(F.data == "menu")
    async def menu(c: CallbackQuery, state: FSMContext):
        await state.clear()
        await edit(c, WELCOME, MENU_KB)

    # ── Список ботов ──
    @dp.callback_query(F.data == "bots")
    async def bots(c: CallbackQuery, state: FSMContext):
        await state.clear()
        rows = await fetch("SELECT * FROM bots WHERE owner_id=? ORDER BY id", c.from_user.id)
        if not rows:
            await edit(c, "У тебя пока нет ботов.", kb([("➕ Добавить бота", "add")], [("⬅️ Назад", "menu")]))
            return
        keyboard = [[(("🟢 " if r["active"] else "🔴 ") + f"@{r['username']}", f"b:{r['id']}")] for r in rows]
        keyboard.append([("⬅️ Назад", "menu")])
        await edit(c, "🤖 <b>Твои боты:</b>", kb(*keyboard))

    @dp.callback_query(F.data.startswith("b:"))
    async def bot_menu(c: CallbackQuery, state: FSMContext):
        await state.clear()
        bid = int(c.data.split(":")[1])
        if not await own_bot(c.from_user.id, bid):
            return await c.answer("Нет доступа", show_alert=True)
        text, markup = await bot_panel(bid)
        await edit(c, text, markup)

    # ── Добавление бота ──
    @dp.callback_query(F.data == "add")
    async def add(c: CallbackQuery, state: FSMContext):
        n = (await one("SELECT COUNT(*) n FROM bots WHERE owner_id=?", c.from_user.id))["n"]
        if n >= MAX_BOTS_PER_USER:
            return await c.answer(f"Максимум {MAX_BOTS_PER_USER} ботов", show_alert=True)
        await state.set_state(AddBot.token)
        await edit(
            c,
            "🔑 Пришли токен своего бота.\n\nПолучить его можно у @BotFather командой /newbot.\n"
            "Сообщение с токеном я сразу удалю.",
            CANCEL_KB,
        )

    @dp.message(AddBot.token)
    async def add_token(m: Message, state: FSMContext):
        token = (m.text or "").strip()
        try:
            await m.delete()
        except Exception:
            pass
        if not TOKEN_RE.match(token):
            return await m.answer("❌ Это не похоже на токен. Попробуй ещё раз.", reply_markup=CANCEL_KB)
        if await one("SELECT id FROM bots WHERE token=?", token):
            return await m.answer("⚠️ Этот бот уже добавлен в конструктор.", reply_markup=CANCEL_KB)
        probe = Bot(token)
        try:
            me = await probe.get_me()
        except Exception:
            return await m.answer("❌ Telegram не принял токен. Проверь его и пришли снова.", reply_markup=CANCEL_KB)
        finally:
            await probe.session.close()
        bid = await run(
            "INSERT INTO bots(owner_id, token, username, name, fallback) VALUES(?,?,?,?,?)",
            m.from_user.id, token, me.username, me.full_name, "Не понимаю 🤷 Попробуй /start",
        )
        await run(
            "INSERT INTO commands(bot_id, name, text) VALUES(?,?,?)",
            bid, "start", "Привет! 👋 Я бот, созданный в LumenКонструктор.",
        )
        await start_child(bid, token)
        await state.clear()
        text, markup = await bot_panel(bid)
        await m.answer(f"✅ Бот @{me.username} подключён и уже работает!\n\n" + text, reply_markup=markup)

    # ── Команды ──
    @dp.callback_query(F.data.startswith("bcs:"))
    async def cmds(c: CallbackQuery, state: FSMContext):
        await state.clear()
        bid = int(c.data.split(":")[1])
        if not await own_bot(c.from_user.id, bid):
            return await c.answer("Нет доступа", show_alert=True)
        rows = await fetch("SELECT * FROM commands WHERE bot_id=? AND name!='start' ORDER BY id", bid)
        keyboard = [[(f"/{r['name']}", f"bc:{r['id']}")] for r in rows]
        keyboard.append([("➕ Новая команда", f"bca:{bid}")])
        keyboard.append([("⬅️ Назад", f"b:{bid}")])
        await edit(c, "📋 <b>Команды бота</b>\nВыбери команду или создай новую.", kb(*keyboard))

    @dp.callback_query(F.data.startswith("bca:"))
    async def cmd_add(c: CallbackQuery, state: FSMContext):
        bid = int(c.data.split(":")[1])
        if not await own_bot(c.from_user.id, bid):
            return await c.answer("Нет доступа", show_alert=True)
        n = (await one("SELECT COUNT(*) n FROM commands WHERE bot_id=?", bid))["n"]
        if n >= MAX_COMMANDS_PER_BOT:
            return await c.answer("Достигнут лимит команд", show_alert=True)
        await state.set_state(AddCmd.name)
        await state.update_data(bid=bid)
        await edit(c, "Как назвать команду? Только латиница, цифры и _ (например: <code>help</code>).", CANCEL_KB)

    @dp.message(AddCmd.name)
    async def cmd_add_name(m: Message, state: FSMContext):
        name = (m.text or "").strip().lstrip("/").lower()
        data = await state.get_data()
        if not CMD_RE.match(name):
            return await m.answer("❌ Только a-z, 0-9 и _, до 32 символов. Ещё раз:", reply_markup=CANCEL_KB)
        if await one("SELECT id FROM commands WHERE bot_id=? AND name=?", data["bid"], name):
            return await m.answer("⚠️ Такая команда уже есть. Придумай другое имя:", reply_markup=CANCEL_KB)
        await state.update_data(name=name)
        await state.set_state(AddCmd.text)
        await m.answer(f"Теперь пришли текст, который бот будет отвечать на /{name}:", reply_markup=CANCEL_KB)

    @dp.message(AddCmd.text)
    async def cmd_add_text(m: Message, state: FSMContext):
        if not m.text or len(m.text) > 4000:
            return await m.answer("Нужен текст до 4000 символов:", reply_markup=CANCEL_KB)
        data = await state.get_data()
        cid = await run("INSERT INTO commands(bot_id, name, text) VALUES(?,?,?)", data["bid"], data["name"], m.text)
        await sync_commands(data["bid"])
        await state.clear()
        text, markup = await cmd_panel(cid)
        await m.answer("✅ Команда создана!\n\n" + text, reply_markup=markup)

    @dp.callback_query(F.data.startswith("bc:"))
    async def cmd_open(c: CallbackQuery, state: FSMContext):
        await state.clear()
        cid = int(c.data.split(":")[1])
        if not await own_cmd(c.from_user.id, cid):
            return await c.answer("Нет доступа", show_alert=True)
        text, markup = await cmd_panel(cid)
        await edit(c, text, markup)

    @dp.callback_query(F.data.startswith("cet:"))
    async def cmd_edit_text(c: CallbackQuery, state: FSMContext):
        cid = int(c.data.split(":")[1])
        if not await own_cmd(c.from_user.id, cid):
            return await c.answer("Нет доступа", show_alert=True)
        await state.set_state(EditText.text)
        await state.update_data(cid=cid)
        await edit(c, "Пришли новый текст ответа:", kb([("✖️ Отмена", f"bc:{cid}")]))

    @dp.message(EditText.text)
    async def cmd_edit_text_done(m: Message, state: FSMContext):
        if not m.text or len(m.text) > 4000:
            return await m.answer("Нужен текст до 4000 символов:")
        cid = (await state.get_data())["cid"]
        cmd = await own_cmd(m.from_user.id, cid)
        if not cmd:
            return await state.clear()
        await run("UPDATE commands SET text=? WHERE id=?", m.text, cid)
        await sync_commands(cmd["bot_id"])
        await state.clear()
        text, markup = await cmd_panel(cid)
        await m.answer("✅ Текст обновлён!\n\n" + text, reply_markup=markup)

    @dp.callback_query(F.data.startswith("cdel:"))
    async def cmd_delete(c: CallbackQuery):
        cid = int(c.data.split(":")[1])
        cmd = await own_cmd(c.from_user.id, cid)
        if not cmd or cmd["name"] == "start":
            return await c.answer("Нельзя удалить", show_alert=True)
        await run("DELETE FROM commands WHERE id=?", cid)
        await sync_commands(cmd["bot_id"])
        await c.answer("Команда удалена")
        c.data = f"bcs:{cmd['bot_id']}"
        await cmds(c, None)  # type: ignore[arg-type]

    # ── Кнопки ──
    @dp.callback_query(F.data.startswith("cba:"))
    async def btn_add(c: CallbackQuery, state: FSMContext):
        cid = int(c.data.split(":")[1])
        if not await own_cmd(c.from_user.id, cid):
            return await c.answer("Нет доступа", show_alert=True)
        n = (await one("SELECT COUNT(*) n FROM buttons WHERE command_id=?", cid))["n"]
        if n >= MAX_BUTTONS_PER_COMMAND:
            return await c.answer("Достигнут лимит кнопок", show_alert=True)
        await state.set_state(AddBtn.text)
        await state.update_data(cid=cid)
        await edit(c, "Какой текст будет на кнопке?", kb([("✖️ Отмена", f"bc:{cid}")]))

    @dp.message(AddBtn.text)
    async def btn_text(m: Message, state: FSMContext):
        label = (m.text or "").strip()
        if not label or len(label) > 60:
            return await m.answer("Текст кнопки — до 60 символов:")
        data = await state.get_data()
        cmd = await own_cmd(m.from_user.id, data["cid"])
        if not cmd:
            return await state.clear()
        await state.update_data(label=label)
        await state.set_state(AddBtn.target)
        others = await fetch("SELECT * FROM commands WHERE bot_id=? ORDER BY id", cmd["bot_id"])
        keyboard = [[(("/" + o["name"]), f"bt:{o['id']}")] for o in others]
        keyboard.append([("✖️ Отмена", f"bc:{data['cid']}")])
        await m.answer(
            "Что будет делать кнопка?\n• пришли <b>ссылку</b> (https://…), или\n• выбери <b>команду</b>, которую она запустит:",
            reply_markup=kb(*keyboard),
        )

    async def finish_btn(user_id: int, state: FSMContext, kind: str, value: str):
        data = await state.get_data()
        await run(
            "INSERT INTO buttons(command_id, text, kind, value) VALUES(?,?,?,?)",
            data["cid"], data["label"], kind, value,
        )
        await state.clear()
        return await cmd_panel(data["cid"])

    @dp.message(AddBtn.target)
    async def btn_url(m: Message, state: FSMContext):
        url = (m.text or "").strip()
        if not re.match(r"^(https?://|tg://)\S+$", url):
            return await m.answer("Нужна ссылка вида https://… или выбери команду кнопкой выше.")
        text, markup = await finish_btn(m.from_user.id, state, "url", url)
        await m.answer("✅ Кнопка добавлена!\n\n" + text, reply_markup=markup)

    @dp.callback_query(AddBtn.target, F.data.startswith("bt:"))
    async def btn_cmd(c: CallbackQuery, state: FSMContext):
        target = int(c.data.split(":")[1])
        if not await own_cmd(c.from_user.id, target):
            return await c.answer("Нет доступа", show_alert=True)
        text, markup = await finish_btn(c.from_user.id, state, "cmd", str(target))
        await edit(c, "✅ Кнопка добавлена!\n\n" + text, markup)

    @dp.callback_query(F.data.startswith("cbd:"))
    async def btn_del_list(c: CallbackQuery):
        cid = int(c.data.split(":")[1])
        if not await own_cmd(c.from_user.id, cid):
            return await c.answer("Нет доступа", show_alert=True)
        rows = await fetch("SELECT * FROM buttons WHERE command_id=?", cid)
        keyboard = [[(f"❌ {r['text']}", f"bx:{r['id']}")] for r in rows]
        keyboard.append([("⬅️ Назад", f"bc:{cid}")])
        await edit(c, "Какую кнопку удалить?", kb(*keyboard))

    @dp.callback_query(F.data.startswith("bx:"))
    async def btn_del(c: CallbackQuery):
        b = await one(
            "SELECT bt.id, bt.command_id FROM buttons bt "
            "JOIN commands c ON c.id=bt.command_id JOIN bots b ON b.id=c.bot_id "
            "WHERE bt.id=? AND b.owner_id=?",
            int(c.data.split(":")[1]), c.from_user.id,
        )
        if not b:
            return await c.answer("Нет доступа", show_alert=True)
        await run("DELETE FROM buttons WHERE id=?", b["id"])
        text, markup = await cmd_panel(b["command_id"])
        await edit(c, text, markup)

    # ── Ответ по умолчанию ──
    @dp.callback_query(F.data.startswith("bfb:"))
    async def fb(c: CallbackQuery, state: FSMContext):
        bid = int(c.data.split(":")[1])
        b = await own_bot(c.from_user.id, bid)
        if not b:
            return await c.answer("Нет доступа", show_alert=True)
        await state.set_state(SetFallback.text)
        await state.update_data(bid=bid)
        cur = html.escape(b["fallback"] or "— (бот молчит)")
        await edit(
            c,
            f"💬 Ответ на непонятные сообщения.\nСейчас: <i>{cur}</i>\n\nПришли новый текст или «-», чтобы бот молчал.",
            kb([("✖️ Отмена", f"b:{bid}")]),
        )

    @dp.message(SetFallback.text)
    async def fb_done(m: Message, state: FSMContext):
        bid = (await state.get_data())["bid"]
        if not await own_bot(m.from_user.id, bid) or not m.text:
            return await state.clear()
        value = None if m.text.strip() == "-" else m.text[:4000]
        await run("UPDATE bots SET fallback=? WHERE id=?", value, bid)
        await state.clear()
        text, markup = await bot_panel(bid)
        await m.answer("✅ Сохранено!\n\n" + text, reply_markup=markup)

    # ── Рассылка ──
    @dp.callback_query(F.data.startswith("bbr:"))
    async def bc_start(c: CallbackQuery, state: FSMContext):
        bid = int(c.data.split(":")[1])
        if not await own_bot(c.from_user.id, bid):
            return await c.answer("Нет доступа", show_alert=True)
        n = (await one("SELECT COUNT(*) n FROM child_users WHERE bot_id=?", bid))["n"]
        await state.set_state(Broadcast.text)
        await state.update_data(bid=bid)
        await edit(c, f"📣 Пришли текст рассылки — получат {n} чел.", kb([("✖️ Отмена", f"b:{bid}")]))

    @dp.message(Broadcast.text)
    async def bc_send(m: Message, state: FSMContext):
        bid = (await state.get_data())["bid"]
        b = await own_bot(m.from_user.id, bid)
        item = running.get(bid)
        if not b or not m.text:
            return await state.clear()
        if not item:
            await state.clear()
            return await m.answer("Бот выключен — включи его, чтобы сделать рассылку.")
        users = await fetch("SELECT user_id FROM child_users WHERE bot_id=?", bid)
        await state.clear()
        status = await m.answer(f"Отправляю… (0/{len(users)})")
        ok = fail = 0
        for u in users:
            try:
                await item[0].send_message(u["user_id"], m.text)
                ok += 1
            except TelegramForbiddenError:
                fail += 1
                await run("DELETE FROM child_users WHERE bot_id=? AND user_id=?", bid, u["user_id"])
            except Exception:
                fail += 1
            await asyncio.sleep(0.05)
        await status.edit_text(f"📣 Готово: доставлено {ok}, не доставлено {fail}.", reply_markup=kb([("⬅️ К боту", f"b:{bid}")]))

    # ── Статистика / вкл-выкл / удаление ──
    @dp.callback_query(F.data.startswith("bst:"))
    async def stats(c: CallbackQuery):
        bid = int(c.data.split(":")[1])
        if not await own_bot(c.from_user.id, bid):
            return await c.answer("Нет доступа", show_alert=True)
        u = (await one("SELECT COUNT(*) n FROM child_users WHERE bot_id=?", bid))["n"]
        cm = (await one("SELECT COUNT(*) n FROM commands WHERE bot_id=?", bid))["n"]
        bt = (await one(
            "SELECT COUNT(*) n FROM buttons WHERE command_id IN (SELECT id FROM commands WHERE bot_id=?)", bid
        ))["n"]
        await edit(c, f"📊 <b>Статистика</b>\n\nПользователей: {u}\nКоманд: {cm}\nКнопок: {bt}", kb([("⬅️ Назад", f"b:{bid}")]))

    @dp.callback_query(F.data.startswith("btg:"))
    async def toggle(c: CallbackQuery):
        bid = int(c.data.split(":")[1])
        b = await own_bot(c.from_user.id, bid)
        if not b:
            return await c.answer("Нет доступа", show_alert=True)
        if b["active"]:
            await stop_child(bid)
            await run("UPDATE bots SET active=0 WHERE id=?", bid)
        else:
            await run("UPDATE bots SET active=1 WHERE id=?", bid)
            await start_child(bid, b["token"])
        text, markup = await bot_panel(bid)
        await edit(c, text, markup)

    @dp.callback_query(F.data.startswith("bdel:"))
    async def delete_ask(c: CallbackQuery):
        bid = int(c.data.split(":")[1])
        if not await own_bot(c.from_user.id, bid):
            return await c.answer("Нет доступа", show_alert=True)
        await edit(
            c,
            "⚠️ Удалить бота из конструктора? Все команды, кнопки и база пользователей пропадут.",
            kb([("🗑 Да, удалить", f"bdy:{bid}")], [("⬅️ Отмена", f"b:{bid}")]),
        )

    @dp.callback_query(F.data.startswith("bdy:"))
    async def delete_yes(c: CallbackQuery):
        bid = int(c.data.split(":")[1])
        if not await own_bot(c.from_user.id, bid):
            return await c.answer("Нет доступа", show_alert=True)
        await stop_child(bid)
        await run("DELETE FROM bots WHERE id=?", bid)
        await edit(c, "🗑 Бот удалён.", kb([("🤖 Мои боты", "bots"), ("⬅️ Меню", "menu")]))


# ───────────────────────── Запуск ─────────────────────────
async def main():
    logging.basicConfig(level=logging.INFO)
    if not MAIN_BOT_TOKEN:
        raise SystemExit("Задай переменную окружения MAIN_BOT_TOKEN")
    await init_db()

    for row in await fetch("SELECT id, token FROM bots WHERE active=1"):
        await start_child(row["id"], row["token"])
    logging.info("Запущено дочерних ботов: %d", len(running))

    main_bot = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    register(dp)
    try:
        await dp.start_polling(main_bot)
    finally:
        for bid in list(running):
            await stop_child(bid)
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
