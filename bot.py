import os
import asyncio
import asyncpg
import logging
from datetime import datetime, date
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters, ConversationHandler
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# States
(MAIN_MENU, SELECT_YEAR, SELECT_MONTH,
 SHOP_MENU, ADD_SHOP, DELETE_SHOP,
 SHOP_ITEMS_MENU, ADD_ITEM, DELETE_ITEM,
 BOZOR_SELECT_DATE, BOZOR_SHOP_MENU, BOZOR_ITEM_SELECT,
 BOZOR_UNIT_SELECT, BOZOR_KG_SELECT, BOZOR_DONA_SELECT,
 NARX_SELECT_DATE, NARX_SHOP_MENU, NARX_ITEM_MENU, NARX_UNIT_CHANGE,
 NARX_KG_CHANGE, NARX_DONA_CHANGE, ENTER_PRICE,
 HISOBOT_DATE, OXIRGI_AMAL_DATE,
 CHANGE_YEAR, CHANGE_MONTH) = range(25)

MONTHS = ["Yanvar","Fevral","Mart","Aprel","May","Iyun",
          "Iyul","Avgust","Sentabr","Oktabr","Noyabr","Dekabr"]

db_pool = None

async def get_db():
    return db_pool

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    async with db_pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            selected_year INT DEFAULT EXTRACT(YEAR FROM NOW()),
            selected_month INT DEFAULT EXTRACT(MONTH FROM NOW()),
            selected_date INT DEFAULT NULL
        );
        CREATE TABLE IF NOT EXISTS shops (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            name TEXT,
            UNIQUE(user_id, name)
        );
        CREATE TABLE IF NOT EXISTS items (
            id SERIAL PRIMARY KEY,
            shop_id INT REFERENCES shops(id) ON DELETE CASCADE,
            name TEXT,
            UNIQUE(shop_id, name)
        );
        CREATE TABLE IF NOT EXISTS cart (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            shop_id INT REFERENCES shops(id) ON DELETE CASCADE,
            item_id INT REFERENCES items(id) ON DELETE CASCADE,
            year INT,
            month INT,
            day INT,
            unit TEXT DEFAULT 'dona',
            quantity FLOAT DEFAULT 1,
            UNIQUE(user_id, item_id, year, month, day)
        );
        CREATE TABLE IF NOT EXISTS prices (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            shop_id INT REFERENCES shops(id) ON DELETE CASCADE,
            item_id INT REFERENCES items(id) ON DELETE CASCADE,
            year INT,
            month INT,
            day INT,
            unit TEXT DEFAULT 'dona',
            quantity FLOAT DEFAULT 1,
            price_per_unit FLOAT,
            total_price FLOAT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)
    logger.info("✅ DB initialized")

async def ensure_user(user_id: int):
    async with db_pool.acquire() as conn:
        now = datetime.now()
        await conn.execute("""
            INSERT INTO users(user_id, selected_year, selected_month)
            VALUES($1,$2,$3) ON CONFLICT(user_id) DO NOTHING
        """, user_id, now.year, now.month)

async def get_user(user_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

async def update_user(user_id: int, **kwargs):
    for k, v in kwargs.items():
        async with db_pool.acquire() as conn:
            await conn.execute(f"UPDATE users SET {k}=$1 WHERE user_id=$2", v, user_id)

async def delete_msg_later(context, chat_id, msg_id, delay=2):
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except:
        pass

def main_menu_keyboard():
    return ReplyKeyboardMarkup([
        ["🛒 Ro'yxat kiritish", "🏪 Bozorga ro'yxat"],
        ["💰 Narxlarni kiritish", "📊 Kunlik hisobot"],
        ["📈 Statistika", "🗑 Oxirgi amalni o'chirish"],
        ["💼 Savat jami narxi"],
        ["📅 Yilni o'zgartirish", "🗓 Oyni o'zgartirish"]
    ], resize_keyboard=True)

def back_keyboard():
    return ReplyKeyboardMarkup([["⬅️ Orqaga"]], resize_keyboard=True)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await ensure_user(user_id)
    user = await get_user(user_id)
    year = user['selected_year']
    month = MONTHS[user['selected_month']-1]
    await update.message.reply_text(
        f"👋 Xush kelibsiz!\n📅 Tanlangan: {month} {year}",
        reply_markup=main_menu_keyboard()
    )
    return MAIN_MENU

# =================== YEAR/MONTH CHANGE ===================

async def change_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = await get_user(update.effective_user.id)
    cur = user['selected_year']
    kb = ReplyKeyboardMarkup([
        [f"◀️ {cur-1}", f"✅ {cur} (hozir)", f"▶️ {cur+1}"],
        ["⬅️ Orqaga"]
    ], resize_keyboard=True)
    await update.message.reply_text(f"📅 Yilni tanlang (hozir: {cur}):", reply_markup=kb)
    return CHANGE_YEAR

async def handle_change_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if text == "⬅️ Orqaga":
        return await go_main(update, context)
    user = await get_user(user_id)
    cur = user['selected_year']
    if f"◀️ {cur-1}" in text:
        new_year = cur - 1
    elif f"▶️ {cur+1}" in text:
        new_year = cur + 1
    else:
        new_year = cur
    await update_user(user_id, selected_year=new_year)
    m = await update.message.reply_text(f"✅ {new_year} yil tanlandi!")
    asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
    return await go_main(update, context)

async def change_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = []
    row = []
    for i, m in enumerate(MONTHS):
        row.append(m)
        if len(row) == 3:
            kb.append(row)
            row = []
    if row:
        kb.append(row)
    kb.append(["⬅️ Orqaga"])
    await update.message.reply_text("🗓 Oyni tanlang:", reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))
    return CHANGE_MONTH

async def handle_change_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if text == "⬅️ Orqaga":
        return await go_main(update, context)
    if text in MONTHS:
        month_num = MONTHS.index(text) + 1
        await update_user(user_id, selected_month=month_num)
        m = await update.message.reply_text(f"✅ {text} oyi tanlandi!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
    return await go_main(update, context)

async def go_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = await get_user(update.effective_user.id)
    year = user['selected_year']
    month = MONTHS[user['selected_month']-1]
    await update.message.reply_text(
        f"🏠 Asosiy menu\n📅 {month} {year}",
        reply_markup=main_menu_keyboard()
    )
    return MAIN_MENU

# =================== RO'YXAT KIRITISH ===================

async def royxat_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with db_pool.acquire() as conn:
        shops = await conn.fetch("SELECT * FROM shops WHERE user_id=$1 ORDER BY name", user_id)
    btns = []
    for s in shops:
        btns.append([s['name']])
    btns.append(["➕ Do'kon qo'shish", "🗑 Do'kon o'chirish"])
    btns.append(["⬅️ Orqaga"])
    await update.message.reply_text(
        "🏪 Do'konlar ro'yxati:",
        reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
    )
    context.user_data['in_royxat'] = True
    return SHOP_MENU

async def shop_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if text == "⬅️ Orqaga":
        context.user_data.pop('in_royxat', None)
        return await go_main(update, context)
    if text == "➕ Do'kon qo'shish":
        await update.message.reply_text("🏪 Do'kon nomini kiriting:", reply_markup=back_keyboard())
        return ADD_SHOP
    if text == "🗑 Do'kon o'chirish":
        async with db_pool.acquire() as conn:
            shops = await conn.fetch("SELECT * FROM shops WHERE user_id=$1", user_id)
        if not shops:
            await update.message.reply_text("❌ Do'konlar yo'q!")
            return SHOP_MENU
        btns = [[s['name']] for s in shops]
        btns.append(["⬅️ Orqaga"])
        await update.message.reply_text("🗑 Qaysi do'konni o'chirmoqchisiz?",
            reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True))
        return DELETE_SHOP
    # Shop selected
    async with db_pool.acquire() as conn:
        shop = await conn.fetchrow("SELECT * FROM shops WHERE user_id=$1 AND name=$2", user_id, text)
    if shop:
        context.user_data['current_shop_id'] = shop['id']
        context.user_data['current_shop_name'] = shop['name']
        return await show_shop_items(update, context, shop['id'], shop['name'])
    return SHOP_MENU

async def add_shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if text == "⬅️ Orqaga":
        return await royxat_menu(update, context)
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("INSERT INTO shops(user_id,name) VALUES($1,$2)", user_id, text)
        m = await update.message.reply_text(f"✅ '{text}' do'koni qo'shildi!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
    except:
        m = await update.message.reply_text("❌ Bu do'kon allaqachon mavjud!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
    return await royxat_menu(update, context)

async def delete_shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if text == "⬅️ Orqaga":
        return await royxat_menu(update, context)
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM shops WHERE user_id=$1 AND name=$2", user_id, text)
    m = await update.message.reply_text(f"✅ '{text}' o'chirildi!")
    asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
    return await royxat_menu(update, context)

async def show_shop_items(update, context, shop_id, shop_name):
    async with db_pool.acquire() as conn:
        items = await conn.fetch("SELECT * FROM items WHERE shop_id=$1 ORDER BY name", shop_id)
    btns = [["➕ Tovar qo'shish", "🗑 Tovarni o'chirish"], ["⬅️ Orqaga"]]
    txt = f"🏪 {shop_name}\n📦 Tovarlar: {len(items)} ta"
    await update.message.reply_text(txt, reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True))
    return SHOP_ITEMS_MENU

async def shop_items_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    shop_id = context.user_data.get('current_shop_id')
    shop_name = context.user_data.get('current_shop_name')
    user_id = update.effective_user.id
    if text == "⬅️ Orqaga":
        return await royxat_menu(update, context)
    if text == "➕ Tovar qo'shish":
        await update.message.reply_text("📦 Tovar nomini kiriting:", reply_markup=back_keyboard())
        return ADD_ITEM
    if text == "🗑 Tovarni o'chirish":
        async with db_pool.acquire() as conn:
            items = await conn.fetch("SELECT * FROM items WHERE shop_id=$1", shop_id)
        if not items:
            m = await update.message.reply_text("❌ Tovarlar yo'q!")
            asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
            return SHOP_ITEMS_MENU
        btns = [[i['name']] for i in items]
        btns.append(["⬅️ Orqaga"])
        await update.message.reply_text("🗑 Qaysi tovarni o'chirmoqchisiz?",
            reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True))
        return DELETE_ITEM
    return SHOP_ITEMS_MENU

async def add_item(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    shop_id = context.user_data.get('current_shop_id')
    shop_name = context.user_data.get('current_shop_name')
    if text == "⬅️ Orqaga":
        return await show_shop_items(update, context, shop_id, shop_name)
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("INSERT INTO items(shop_id,name) VALUES($1,$2)", shop_id, text)
        m = await update.message.reply_text(f"✅ '{text}' tovar qo'shildi!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
    except:
        m = await update.message.reply_text("❌ Bu tovar allaqachon mavjud!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
    return await show_shop_items(update, context, shop_id, shop_name)

async def delete_item(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    shop_id = context.user_data.get('current_shop_id')
    shop_name = context.user_data.get('current_shop_name')
    if text == "⬅️ Orqaga":
        return await show_shop_items(update, context, shop_id, shop_name)
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM items WHERE shop_id=$1 AND name=$2", shop_id, text)
    m = await update.message.reply_text(f"✅ '{text}' o'chirildi!")
    asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
    return await show_shop_items(update, context, shop_id, shop_name)

# =================== BOZORGA RO'YXAT ===================

async def bozor_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await get_user(user_id)
    year = user['selected_year']
    month = user['selected_month']
    btns = []
    row = []
    for d in range(1, 32):
        row.append(str(d))
        if len(row) == 7:
            btns.append(row)
            row = []
    if row:
        btns.append(row)
    btns.append(["⬅️ Orqaga"])
    await update.message.reply_text(
        f"📅 {MONTHS[month-1]} {year}\nBozorga sanani tanlang:",
        reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
    )
    return BOZOR_SELECT_DATE

async def bozor_date_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if text == "⬅️ Orqaga":
        return await go_main(update, context)
    try:
        day = int(text)
        context.user_data['bozor_day'] = day
        user = await get_user(user_id)
        context.user_data['bozor_year'] = user['selected_year']
        context.user_data['bozor_month'] = user['selected_month']
        return await show_bozor_shops(update, context)
    except:
        return BOZOR_SELECT_DATE

async def show_bozor_shops(update, context):
    user_id = update.effective_user.id
    day = context.user_data.get('bozor_day')
    year = context.user_data.get('bozor_year')
    month = context.user_data.get('bozor_month')
    async with db_pool.acquire() as conn:
        shops = await conn.fetch("SELECT * FROM shops WHERE user_id=$1 ORDER BY name", user_id)
        cart_items = await conn.fetch("""
            SELECT c.*, i.name as iname, s.name as sname
            FROM cart c
            JOIN items i ON c.item_id=i.id
            JOIN shops s ON c.shop_id=s.id
            WHERE c.user_id=$1 AND c.year=$2 AND c.month=$3 AND c.day=$4
        """, user_id, year, month, day)
    cart_count = len(cart_items)
    btns = [[s['name']] for s in shops]
    btns.append(["🗑 Savatdan o'chirish", f"🛒 Savat ({cart_count})"])
    btns.append(["⬅️ Orqaga"])
    txt = f"🏪 Do'konni tanlang\n📅 {day}.{month:02d}.{year}\n🛒 Savatda: {cart_count} ta tovar"
    await update.message.reply_text(txt, reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True))
    return BOZOR_SHOP_MENU

async def bozor_shop_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    day = context.user_data.get('bozor_day')
    year = context.user_data.get('bozor_year')
    month = context.user_data.get('bozor_month')

    if text == "⬅️ Orqaga":
        return await bozor_menu(update, context)

    if text == f"🛒 Savat ({await get_cart_count(user_id,year,month,day)})":
        return await show_cart(update, context)

    # Handle dynamic cart button
    if text.startswith("🛒 Savat"):
        return await show_cart(update, context)

    if text == "🗑 Savatdan o'chirish":
        async with db_pool.acquire() as conn:
            items_in_cart = await conn.fetch("""
                SELECT c.id, i.name as iname, s.name as sname, c.quantity, c.unit
                FROM cart c
                JOIN items i ON c.item_id=i.id
                JOIN shops s ON c.shop_id=s.id
                WHERE c.user_id=$1 AND c.year=$2 AND c.month=$3 AND c.day=$4
                ORDER BY s.name, i.name
            """, user_id, year, month, day)
        if not items_in_cart:
            m = await update.message.reply_text("❌ Savat bo'sh!")
            asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
            return await show_bozor_shops(update, context)
        context.user_data['delete_from_cart'] = True
        btns = [[f"🗑 {i['iname']} ({i['quantity']}{i['unit']}) - {i['sname']}"] for i in items_in_cart]
        btns.append(["⬅️ Orqaga"])
        await update.message.reply_text("O'chirmoqchi bo'lgan tovarni tanlang:",
            reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True))
        return BOZOR_ITEM_SELECT

    # Shop selected
    async with db_pool.acquire() as conn:
        shop = await conn.fetchrow("SELECT * FROM shops WHERE user_id=$1 AND name=$2", user_id, text)
    if not shop:
        return BOZOR_SHOP_MENU

    context.user_data['bozor_shop_id'] = shop['id']
    context.user_data['bozor_shop_name'] = shop['name']

    async with db_pool.acquire() as conn:
        items = await conn.fetch("SELECT * FROM items WHERE shop_id=$1 ORDER BY name", shop['id'])
        cart_items = await conn.fetch("""
            SELECT i.name FROM cart c JOIN items i ON c.item_id=i.id
            WHERE c.user_id=$1 AND c.shop_id=$2 AND c.year=$3 AND c.month=$4 AND c.day=$5
        """, user_id, shop['id'], year, month, day)

    cart_names = [ci['name'] for ci in cart_items]
    btns = []
    for item in items:
        prefix = "✅ " if item['name'] in cart_names else ""
        btns.append([f"{prefix}{item['name']}"])
    btns.append(["➕ Tovar qo'shish (bu do'kon)"])
    btns.append(["⬅️ Orqaga"])
    await update.message.reply_text(
        f"🏪 {shop['name']}\nNima xarid qilmoqchisiz?",
        reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
    )
    return BOZOR_ITEM_SELECT

async def get_cart_count(user_id, year, month, day):
    async with db_pool.acquire() as conn:
        r = await conn.fetchval("""
            SELECT COUNT(*) FROM cart WHERE user_id=$1 AND year=$2 AND month=$3 AND day=$4
        """, user_id, year, month, day)
    return r or 0

async def bozor_item_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    day = context.user_data.get('bozor_day')
    year = context.user_data.get('bozor_year')
    month = context.user_data.get('bozor_month')
    shop_id = context.user_data.get('bozor_shop_id')
    shop_name = context.user_data.get('bozor_shop_name')

    if text == "⬅️ Orqaga":
        return await show_bozor_shops(update, context)

    # Delete from cart mode
    if context.user_data.get('delete_from_cart') and text.startswith("🗑 "):
        item_name = text.split("🗑 ")[1].split(" (")[0]
        async with db_pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM cart WHERE user_id=$1 AND year=$2 AND month=$3 AND day=$4
                AND item_id=(SELECT i.id FROM items i JOIN shops s ON i.shop_id=s.id
                WHERE i.name=$5 AND s.user_id=$1 LIMIT 1)
            """, user_id, year, month, day, item_name)
        context.user_data.pop('delete_from_cart', None)
        m = await update.message.reply_text(f"✅ '{item_name}' savatdan o'chirildi!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
        return await show_bozor_shops(update, context)

    if text == "➕ Tovar qo'shish (bu do'kon)":
        await update.message.reply_text(
            f"🏪 {shop_name}\nBu do'kondan nima olmoqchisiz? Tovar nomini kiriting:",
            reply_markup=back_keyboard()
        )
        context.user_data['adding_custom_bozor_item'] = True
        return BOZOR_ITEM_SELECT

    if context.user_data.get('adding_custom_bozor_item'):
        if text == "⬅️ Orqaga":
            context.user_data.pop('adding_custom_bozor_item', None)
            return await show_bozor_shops(update, context)
        # Add new item to shop and cart
        async with db_pool.acquire() as conn:
            try:
                await conn.execute("INSERT INTO items(shop_id,name) VALUES($1,$2)", shop_id, text)
            except:
                pass
            item = await conn.fetchrow("SELECT * FROM items WHERE shop_id=$1 AND name=$2", shop_id, text)
        context.user_data['bozor_item_id'] = item['id']
        context.user_data['bozor_item_name'] = text
        context.user_data.pop('adding_custom_bozor_item', None)
        return await ask_unit(update, context)

    # Item selected (remove ✅ prefix if any)
    item_name = text.replace("✅ ", "")
    async with db_pool.acquire() as conn:
        item = await conn.fetchrow("SELECT * FROM items WHERE shop_id=$1 AND name=$2", shop_id, item_name)
    if not item:
        return BOZOR_ITEM_SELECT

    context.user_data['bozor_item_id'] = item['id']
    context.user_data['bozor_item_name'] = item_name
    return await ask_unit(update, context)

async def ask_unit(update, context):
    item_name = context.user_data.get('bozor_item_name')
    # Check if already in cart
    user_id = update.effective_user.id
    day = context.user_data.get('bozor_day')
    year = context.user_data.get('bozor_year')
    month = context.user_data.get('bozor_month')
    item_id = context.user_data.get('bozor_item_id')

    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow("""
            SELECT * FROM cart WHERE user_id=$1 AND item_id=$2 AND year=$3 AND month=$4 AND day=$5
        """, user_id, item_id, year, month, day)

    if existing:
        unit_btn = f"✅ {existing['unit']} ({existing['quantity']})"
        btns = [[unit_btn], ["kg", "dona"], ["⬅️ Orqaga"]]
    else:
        btns = [["kg", "dona"], ["⬅️ Orqaga"]]

    await update.message.reply_text(
        f"📦 {item_name}\nBirligini tanlang:",
        reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
    )
    return BOZOR_UNIT_SELECT

async def bozor_unit_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "⬅️ Orqaga":
        return await show_bozor_shops(update, context)
    if text == "kg" or "kg" in text:
        context.user_data['bozor_unit'] = 'kg'
        btns = [["-10", "-0.5", "0.5kg", "+0.5", "+10"], ["⬅️ Orqaga"]]
        await update.message.reply_text(
            f"⚖️ Necha kg?",
            reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
        )
        context.user_data['bozor_qty'] = 0.5
        return BOZOR_KG_SELECT
    if text == "dona" or "dona" in text:
        context.user_data['bozor_unit'] = 'dona'
        btns = [["-1", "1dona", "+1"], ["⬅️ Orqaga"]]
        await update.message.reply_text(
            f"🔢 Nechta dona?",
            reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
        )
        context.user_data['bozor_qty'] = 1
        return BOZOR_DONA_SELECT
    return BOZOR_UNIT_SELECT

async def bozor_kg_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "⬅️ Orqaga":
        return await ask_unit(update, context)
    qty = context.user_data.get('bozor_qty', 0.5)
    if text == "+0.5":
        qty += 0.5
    elif text == "-0.5":
        qty = max(0.5, qty - 0.5)
    elif text == "+10":
        qty += 10
    elif text == "-10":
        qty = max(0.5, qty - 10)
    elif text == "0.5kg":
        pass
    else:
        try:
            qty = float(text)
        except:
            pass
    context.user_data['bozor_qty'] = qty
    btns = [["-10", "-0.5", f"{qty}kg", "+0.5", "+10"], ["✅ Saqlash"], ["⬅️ Orqaga"]]
    await update.message.reply_text(
        f"⚖️ {qty} kg tanlandi",
        reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
    )
    return BOZOR_KG_SELECT

async def bozor_dona_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "⬅️ Orqaga":
        return await ask_unit(update, context)
    qty = context.user_data.get('bozor_qty', 1)
    if text == "+1":
        qty += 1
    elif text == "-1":
        qty = max(1, qty - 1)
    elif text == "1dona":
        pass
    else:
        try:
            qty = int(text)
        except:
            pass
    context.user_data['bozor_qty'] = qty
    btns = [["-1", f"{qty}dona", "+1"], ["✅ Saqlash"], ["⬅️ Orqaga"]]
    await update.message.reply_text(
        f"🔢 {qty} dona tanlandi",
        reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
    )
    return BOZOR_DONA_SELECT

async def save_to_cart(update, context):
    user_id = update.effective_user.id
    day = context.user_data.get('bozor_day')
    year = context.user_data.get('bozor_year')
    month = context.user_data.get('bozor_month')
    shop_id = context.user_data.get('bozor_shop_id')
    item_id = context.user_data.get('bozor_item_id')
    item_name = context.user_data.get('bozor_item_name')
    unit = context.user_data.get('bozor_unit', 'dona')
    qty = context.user_data.get('bozor_qty', 1)

    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO cart(user_id,shop_id,item_id,year,month,day,unit,quantity)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT(user_id,item_id,year,month,day)
            DO UPDATE SET unit=$7, quantity=$8
        """, user_id, shop_id, item_id, year, month, day, unit, qty)

    m = await update.message.reply_text(f"✅ '{item_name}' {qty}{unit} savatga qo'shildi!")
    asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
    return await show_bozor_shops(update, context)

async def bozor_kg_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "✅ Saqlash":
        return await save_to_cart(update, context)
    return await bozor_kg_handler(update, context)

async def bozor_dona_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "✅ Saqlash":
        return await save_to_cart(update, context)
    return await bozor_dona_handler(update, context)

async def show_cart(update, context):
    user_id = update.effective_user.id
    day = context.user_data.get('bozor_day')
    year = context.user_data.get('bozor_year')
    month = context.user_data.get('bozor_month')
    async with db_pool.acquire() as conn:
        items = await conn.fetch("""
            SELECT c.*, i.name as iname, s.name as sname
            FROM cart c JOIN items i ON c.item_id=i.id JOIN shops s ON c.shop_id=s.id
            WHERE c.user_id=$1 AND c.year=$2 AND c.month=$3 AND c.day=$4
            ORDER BY s.name, i.name
        """, user_id, year, month, day)
    if not items:
        m = await update.message.reply_text("🛒 Savat bo'sh!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
        return await show_bozor_shops(update, context)

    txt = f"🛒 Savat - {day}.{month:02d}.{year}\n\n"
    cur_shop = ""
    for i in items:
        if i['sname'] != cur_shop:
            cur_shop = i['sname']
            txt += f"\n🏪 {cur_shop}:\n"
        txt += f"  📦 {i['iname']} - {i['quantity']}{i['unit']}\n"
    await update.message.reply_text(txt, reply_markup=ReplyKeyboardMarkup([["⬅️ Orqaga"]], resize_keyboard=True))
    return BOZOR_SHOP_MENU

# =================== NARXLARNI KIRITISH ===================

async def narx_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await get_user(user_id)
    year = user['selected_year']
    month = user['selected_month']
    async with db_pool.acquire() as conn:
        days_with_cart = await conn.fetch("""
            SELECT DISTINCT day FROM cart
            WHERE user_id=$1 AND year=$2 AND month=$3 ORDER BY day
        """, user_id, year, month)
    if not days_with_cart:
        m = await update.message.reply_text("❌ Hech qanday sana uchun savat to'ldirilmagan!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
        return await go_main(update, context)
    btns = [[str(d['day'])] for d in days_with_cart]
    btns.append(["⬅️ Orqaga"])
    await update.message.reply_text(
        f"📅 {MONTHS[month-1]} {year}\nNarx kiritish uchun sanani tanlang:",
        reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
    )
    return NARX_SELECT_DATE

async def narx_date_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if text == "⬅️ Orqaga":
        return await go_main(update, context)
    try:
        day = int(text)
        context.user_data['narx_day'] = day
        user = await get_user(user_id)
        context.user_data['narx_year'] = user['selected_year']
        context.user_data['narx_month'] = user['selected_month']
        return await show_narx_shops(update, context)
    except:
        return NARX_SELECT_DATE

async def show_narx_shops(update, context):
    user_id = update.effective_user.id
    day = context.user_data.get('narx_day')
    year = context.user_data.get('narx_year')
    month = context.user_data.get('narx_month')
    async with db_pool.acquire() as conn:
        shops = await conn.fetch("""
            SELECT DISTINCT s.id, s.name FROM cart c
            JOIN shops s ON c.shop_id=s.id
            WHERE c.user_id=$1 AND c.year=$2 AND c.month=$3 AND c.day=$4
            ORDER BY s.name
        """, user_id, year, month, day)
    btns = [[s['name']] for s in shops]
    btns.append(["⬅️ Orqaga"])
    await update.message.reply_text(
        f"📅 {day}.{month:02d}.{year}\nDo'konni tanlang:",
        reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
    )
    return NARX_SHOP_MENU

async def narx_shop_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if text == "⬅️ Orqaga":
        return await narx_menu(update, context)
    async with db_pool.acquire() as conn:
        shop = await conn.fetchrow("SELECT * FROM shops WHERE user_id=$1 AND name=$2", user_id, text)
    if not shop:
        return NARX_SHOP_MENU
    context.user_data['narx_shop_id'] = shop['id']
    context.user_data['narx_shop_name'] = shop['name']
    return await show_narx_items(update, context)

async def show_narx_items(update, context):
    user_id = update.effective_user.id
    day = context.user_data.get('narx_day')
    year = context.user_data.get('narx_year')
    month = context.user_data.get('narx_month')
    shop_id = context.user_data.get('narx_shop_id')
    shop_name = context.user_data.get('narx_shop_name')

    async with db_pool.acquire() as conn:
        cart_items = await conn.fetch("""
            SELECT c.*, i.name as iname FROM cart c JOIN items i ON c.item_id=i.id
            WHERE c.user_id=$1 AND c.shop_id=$2 AND c.year=$3 AND c.month=$4 AND c.day=$5
            ORDER BY i.name
        """, user_id, shop_id, year, month, day)
        price_records = await conn.fetch("""
            SELECT item_id FROM prices
            WHERE user_id=$1 AND shop_id=$2 AND year=$3 AND month=$4 AND day=$5
        """, user_id, shop_id, year, month, day)

    priced_ids = {p['item_id'] for p in price_records}
    btns = []
    for ci in cart_items:
        prefix = "✅ " if ci['item_id'] in priced_ids else ""
        btns.append([f"{prefix}{ci['iname']} ({ci['quantity']}{ci['unit']})"])
    btns.append(["⬅️ Orqaga"])
    await update.message.reply_text(
        f"🏪 {shop_name} - {day}.{month:02d}.{year}\nTovarni tanlang:",
        reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
    )
    return NARX_ITEM_MENU

async def narx_item_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    shop_id = context.user_data.get('narx_shop_id')
    day = context.user_data.get('narx_day')
    year = context.user_data.get('narx_year')
    month = context.user_data.get('narx_month')

    if text == "⬅️ Orqaga":
        return await show_narx_shops(update, context)

    # Parse item name (remove prefix and qty suffix)
    clean = text.replace("✅ ", "")
    item_name = clean.split(" (")[0]

    async with db_pool.acquire() as conn:
        item = await conn.fetchrow("""
            SELECT i.* FROM items i WHERE i.shop_id=$1 AND i.name=$2
        """, shop_id, item_name)
        if not item:
            return NARX_ITEM_MENU
        cart_item = await conn.fetchrow("""
            SELECT * FROM cart WHERE user_id=$1 AND item_id=$2 AND year=$3 AND month=$4 AND day=$5
        """, user_id, item['id'], year, month, day)
        last_price = await conn.fetchrow("""
            SELECT price_per_unit, unit FROM prices
            WHERE user_id=$1 AND item_id=$2
            ORDER BY year DESC, month DESC, day DESC LIMIT 1
        """, user_id, item['id'])

    context.user_data['narx_item_id'] = item['id']
    context.user_data['narx_item_name'] = item_name
    if cart_item:
        context.user_data['narx_unit'] = cart_item['unit']
        context.user_data['narx_qty'] = cart_item['quantity']

    qty = cart_item['quantity'] if cart_item else 1
    unit = cart_item['unit'] if cart_item else 'dona'
    last_price_txt = f"\n⬇️ Oxirgi narx: {last_price['price_per_unit']:,.0f} so'm/{last_price['unit']}" if last_price else ""

    btns = [
        [f"💰 Narxini kiritish ({qty}{unit})"],
        ["⚖️ kg/dona o'zgartirish"],
        ["⬅️ Orqaga"]
    ]
    await update.message.reply_text(
        f"📦 {item_name}\n📊 {qty}{unit} oldingiz{last_price_txt}\n\nNimani qilmoqchisiz?",
        reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
    )
    return NARX_ITEM_MENU

async def narx_item_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    item_name = context.user_data.get('narx_item_name')
    qty = context.user_data.get('narx_qty', 1)
    unit = context.user_data.get('narx_unit', 'dona')

    if text == "⬅️ Orqaga":
        return await show_narx_items(update, context)

    if text.startswith("💰 Narxini kiritish"):
        last_price = await get_last_price(
            update.effective_user.id,
            context.user_data.get('narx_item_id')
        )
        last_txt = f"\n⬇️ Oxirgi narx: {last_price:,.0f} so'm" if last_price else ""
        await update.message.reply_text(
            f"📦 {item_name} - {qty}{unit} oldingiz\n💰 1{unit} narxini kiriting (faqat raqam){last_txt}:",
            reply_markup=back_keyboard()
        )
        context.user_data['entering_price'] = True
        return ENTER_PRICE

    if text == "⚖️ kg/dona o'zgartirish":
        btns = [["kg", "dona"], ["⬅️ Orqaga"]]
        await update.message.reply_text("Birlikni tanlang:", reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True))
        return NARX_UNIT_CHANGE

    return NARX_ITEM_MENU

async def get_last_price(user_id, item_id):
    async with db_pool.acquire() as conn:
        r = await conn.fetchrow("""
            SELECT price_per_unit FROM prices WHERE user_id=$1 AND item_id=$2
            ORDER BY year DESC, month DESC, day DESC LIMIT 1
        """, user_id, item_id)
    return r['price_per_unit'] if r else None

async def narx_unit_change(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    item_id = context.user_data.get('narx_item_id')
    item_name = context.user_data.get('narx_item_name')

    if text == "⬅️ Orqaga":
        return await narx_item_handler(update, context)

    if text in ["kg", "dona"]:
        context.user_data['narx_unit'] = text
        context.user_data['changing_unit'] = True
        if text == "kg":
            btns = [["-10", "-0.5", "0.5kg", "+0.5", "+10"], ["✅ Saqlash"], ["⬅️ Orqaga"]]
            context.user_data['narx_qty'] = 0.5
        else:
            btns = [["-1", "1dona", "+1"], ["✅ Saqlash"], ["⬅️ Orqaga"]]
            context.user_data['narx_qty'] = 1
        await update.message.reply_text(
            f"📦 {item_name} - necha {text}?",
            reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
        )
        if text == "kg":
            return NARX_KG_CHANGE
        return NARX_DONA_CHANGE
    return NARX_UNIT_CHANGE

async def narx_kg_change(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "⬅️ Orqaga":
        return await show_narx_items(update, context)
    qty = context.user_data.get('narx_qty', 0.5)
    if text == "+0.5": qty += 0.5
    elif text == "-0.5": qty = max(0.5, qty - 0.5)
    elif text == "+10": qty += 10
    elif text == "-10": qty = max(0.5, qty - 10)
    if text == "✅ Saqlash":
        await update_cart_unit(update.effective_user.id, context, 'kg', qty)
        return await show_narx_items(update, context)
    context.user_data['narx_qty'] = qty
    btns = [["-10", "-0.5", f"{qty}kg", "+0.5", "+10"], ["✅ Saqlash"], ["⬅️ Orqaga"]]
    await update.message.reply_text(f"⚖️ {qty} kg", reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True))
    return NARX_KG_CHANGE

async def narx_dona_change(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "⬅️ Orqaga":
        return await show_narx_items(update, context)
    qty = context.user_data.get('narx_qty', 1)
    if text == "+1": qty += 1
    elif text == "-1": qty = max(1, qty - 1)
    if text == "✅ Saqlash":
        await update_cart_unit(update.effective_user.id, context, 'dona', qty)
        return await show_narx_items(update, context)
    context.user_data['narx_qty'] = qty
    btns = [["-1", f"{qty}dona", "+1"], ["✅ Saqlash"], ["⬅️ Orqaga"]]
    await update.message.reply_text(f"🔢 {qty} dona", reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True))
    return NARX_DONA_CHANGE

async def update_cart_unit(user_id, context, unit, qty):
    item_id = context.user_data.get('narx_item_id')
    day = context.user_data.get('narx_day')
    year = context.user_data.get('narx_year')
    month = context.user_data.get('narx_month')
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE cart SET unit=$1, quantity=$2
            WHERE user_id=$3 AND item_id=$4 AND year=$5 AND month=$6 AND day=$7
        """, unit, qty, user_id, item_id, year, month, day)
    context.user_data['narx_unit'] = unit
    context.user_data['narx_qty'] = qty

async def enter_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if text == "⬅️ Orqaga":
        context.user_data.pop('entering_price', None)
        return await show_narx_items(update, context)
    try:
        price = float(text.replace(",","").replace(" ",""))
        item_id = context.user_data.get('narx_item_id')
        item_name = context.user_data.get('narx_item_name')
        shop_id = context.user_data.get('narx_shop_id')
        day = context.user_data.get('narx_day')
        year = context.user_data.get('narx_year')
        month = context.user_data.get('narx_month')
        unit = context.user_data.get('narx_unit', 'dona')
        qty = context.user_data.get('narx_qty', 1)
        total = price * qty

        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO prices(user_id,shop_id,item_id,year,month,day,unit,quantity,price_per_unit,total_price)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT DO NOTHING
            """, user_id, shop_id, item_id, year, month, day, unit, qty, price, total)

        m = await update.message.reply_text(
            f"✅ {item_name}: {qty}{unit} × {price:,.0f} = {total:,.0f} so'm saqlandi!"
        )
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
        context.user_data.pop('entering_price', None)
        return await show_narx_items(update, context)
    except:
        m = await update.message.reply_text("❌ Faqat raqam kiriting!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
        return ENTER_PRICE

# =================== KUNLIK HISOBOT ===================

async def hisobot_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await get_user(user_id)
    year = user['selected_year']
    month = user['selected_month']
    async with db_pool.acquire() as conn:
        dates = await conn.fetch("""
            SELECT DISTINCT day FROM prices
            WHERE user_id=$1 AND year=$2 AND month=$3 ORDER BY day
        """, user_id, year, month)
    if not dates:
        m = await update.message.reply_text("❌ Hali narx kiritilmagan!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
        return await go_main(update, context)
    btns = [[f"{d['day']:02d}.{month:02d}.{year}"] for d in dates]
    btns.append(["⬅️ Orqaga"])
    await update.message.reply_text("📅 Sanani tanlang:", reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True))
    return HISOBOT_DATE

async def hisobot_date_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if text == "⬅️ Orqaga":
        return await go_main(update, context)
    try:
        parts = text.split(".")
        day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
    except:
        return HISOBOT_DATE

    async with db_pool.acquire() as conn:
        records = await conn.fetch("""
            SELECT p.*, i.name as iname, s.name as sname
            FROM prices p JOIN items i ON p.item_id=i.id JOIN shops s ON p.shop_id=s.id
            WHERE p.user_id=$1 AND p.year=$2 AND p.month=$3 AND p.day=$4
            ORDER BY s.name, i.name
        """, user_id, year, month, day)

    if not records:
        m = await update.message.reply_text("❌ Bu sana uchun ma'lumot yo'q!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
        return HISOBOT_DATE

    txt = f"📊 {day:02d}.{month:02d}.{year} Hisobot\n\n"
    cur_shop = ""
    shop_total = 0
    grand_total = 0
    for r in records:
        if r['sname'] != cur_shop:
            if cur_shop:
                txt += f"📦 {cur_shop} jami: {shop_total:,.0f} so'm\n\n"
            cur_shop = r['sname']
            shop_total = 0
            txt += f"🏪 {cur_shop}:\n"
        txt += f"  • {r['iname']} {r['quantity']}{r['unit']} - {r['total_price']:,.0f} so'm\n"
        shop_total += r['total_price']
        grand_total += r['total_price']
    txt += f"📦 {cur_shop} jami: {shop_total:,.0f} so'm\n\n"
    txt += f"💰 JAMI: {grand_total:,.0f} so'm"

    await update.message.reply_text(txt, reply_markup=ReplyKeyboardMarkup([["⬅️ Orqaga"]], resize_keyboard=True))
    return HISOBOT_DATE

# =================== STATISTIKA ===================

async def statistika(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await get_user(user_id)
    year = user['selected_year']
    month = user['selected_month']

    async with db_pool.acquire() as conn:
        dates = await conn.fetch("""
            SELECT DISTINCT day FROM prices
            WHERE user_id=$1 AND year=$2 AND month=$3 ORDER BY day DESC LIMIT 3
        """, user_id, year, month)

    if len(dates) < 1:
        m = await update.message.reply_text("❌ Yetarli ma'lumot yo'q!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
        return await go_main(update, context)

    days_sorted = sorted([d['day'] for d in dates])
    latest_day = days_sorted[-1]
    prev_days = days_sorted[:-1]

    async with db_pool.acquire() as conn:
        latest = await conn.fetch("""
            SELECT p.*, i.name as iname, s.name as sname
            FROM prices p JOIN items i ON p.item_id=i.id JOIN shops s ON p.shop_id=s.id
            WHERE p.user_id=$1 AND p.year=$2 AND p.month=$3 AND p.day=$4
            ORDER BY s.name, i.name
        """, user_id, year, month, latest_day)

    txt = f"📈 Statistika - {latest_day:02d}.{month:02d}.{year}\n\n"
    rose = []
    fell = []
    cur_shop = ""

    for r in latest:
        if r['sname'] != cur_shop:
            cur_shop = r['sname']
            txt += f"\n🏪 {cur_shop}:\n"

        cur_price = r['price_per_unit']
        prev_info = []
        trend = "✅"

        for pd in prev_days:
            async with db_pool.acquire() as conn:
                prev = await conn.fetchrow("""
                    SELECT price_per_unit, day FROM prices
                    WHERE user_id=$1 AND item_id=$2 AND year=$3 AND month=$4 AND day=$5
                """, user_id, r['item_id'], year, month, pd)
            if prev:
                prev_info.append(f"{pd:02d}.{month:02d}.{year}-{prev['price_per_unit']:,.0f} so'm")
                if cur_price > prev['price_per_unit']:
                    trend = "❌↗️"
                elif cur_price < prev['price_per_unit'] and trend != "❌↗️":
                    trend = "⭕️↘️"

        if trend == "❌↗️":
            rose.append(f"{r['iname']}")
        elif trend == "⭕️↘️":
            fell.append(f"{r['iname']}")

        prev_txt = " / ".join(prev_info) if prev_info else "Ma'lumot yo'q"
        txt += f"  {r['iname']} {trend}\n"
        txt += f"    💰 {cur_price:,.0f} so'm / ({prev_txt})\n"

    txt += f"\n📊 Xulosa:\n"
    if rose:
        txt += f"❌↗️ Oshgan: {', '.join(rose)}\n"
    if fell:
        txt += f"⭕️↘️ Tushgan: {', '.join(fell)}\n"

    await update.message.reply_text(txt, reply_markup=main_menu_keyboard())
    return MAIN_MENU

# =================== OXIRGI AMALNI O'CHIRISH ===================

async def oxirgi_amal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await get_user(user_id)
    year = user['selected_year']
    month = user['selected_month']

    async with db_pool.acquire() as conn:
        dates = await conn.fetch("""
            SELECT DISTINCT day FROM prices
            WHERE user_id=$1 AND year=$2 AND month=$3 ORDER BY day
        """, user_id, year, month)

    if not dates:
        m = await update.message.reply_text("❌ O'chirish uchun ma'lumot yo'q!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
        return await go_main(update, context)

    btns = [[f"{d['day']:02d}.{month:02d}.{year}"] for d in dates]
    btns.append(["⬅️ Orqaga"])
    await update.message.reply_text(
        "🗑 Qaysi sanadagi oxirgi amalni o'chirmoqchisiz?",
        reply_markup=ReplyKeyboardMarkup(btns, resize_keyboard=True)
    )
    return OXIRGI_AMAL_DATE

async def oxirgi_amal_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if text == "⬅️ Orqaga":
        return await go_main(update, context)
    try:
        parts = text.split(".")
        day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
    except:
        return OXIRGI_AMAL_DATE

    async with db_pool.acquire() as conn:
        last = await conn.fetchrow("""
            SELECT p.id, i.name as iname FROM prices p JOIN items i ON p.item_id=i.id
            WHERE p.user_id=$1 AND p.year=$2 AND p.month=$3 AND p.day=$4
            ORDER BY p.created_at DESC LIMIT 1
        """, user_id, year, month, day)

        if not last:
            m = await update.message.reply_text("❌ Bu sana uchun ma'lumot yo'q!")
            asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
            return OXIRGI_AMAL_DATE

        await conn.execute("DELETE FROM prices WHERE id=$1", last['id'])

    m = await update.message.reply_text(f"✅ '{last['iname']}' narxi o'chirildi!")
    asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
    return OXIRGI_AMAL_DATE

# =================== SAVAT JAMI NARXI ===================

async def savat_jami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await get_user(user_id)
    year = user['selected_year']
    month = user['selected_month']

    async with db_pool.acquire() as conn:
        days = await conn.fetch("""
            SELECT DISTINCT day FROM cart
            WHERE user_id=$1 AND year=$2 AND month=$3 ORDER BY day
        """, user_id, year, month)

    if not days:
        m = await update.message.reply_text("❌ Savat bo'sh!")
        asyncio.create_task(delete_msg_later(context, update.effective_chat.id, m.message_id))
        return MAIN_MENU

    txt = f"💼 Savat Jami Narxi - {MONTHS[month-1]} {year}\n\n"
    for d in days:
        day = d['day']
        async with db_pool.acquire() as conn:
            items = await conn.fetch("""
                SELECT c.*, i.name as iname, s.name as sname,
                (SELECT price_per_unit FROM prices p2
                 WHERE p2.user_id=c.user_id AND p2.item_id=c.item_id
                 ORDER BY p2.year DESC, p2.month DESC, p2.day DESC LIMIT 1) as last_price
                FROM cart c JOIN items i ON c.item_id=i.id JOIN shops s ON c.shop_id=s.id
                WHERE c.user_id=$1 AND c.year=$2 AND c.month=$3 AND c.day=$4
                ORDER BY s.name, i.name
            """, user_id, year, month, day)

        day_total = sum((i['last_price'] or 0) * i['quantity'] for i in items)
        txt += f"📅 {day:02d}.{month:02d}.{year}: {day_total:,.0f} so'm\n"

    await update.message.reply_text(txt, reply_markup=main_menu_keyboard())
    return MAIN_MENU

# =================== HEALTH SERVER ===================

async def health_server():
    from aiohttp import web
    app = web.Application()
    async def health(request):
        return web.Response(text="OK")
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 10000)))
    await site.start()
    logger.info("✅ Health server :10000")

# =================== MAIN ===================

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_db())
    loop.run_until_complete(health_server())

    app = Application.builder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            MAIN_MENU: [
                MessageHandler(filters.Regex("^🛒 Ro'yxat kiritish$"), royxat_menu),
                MessageHandler(filters.Regex("^🏪 Bozorga ro'yxat$"), bozor_menu),
                MessageHandler(filters.Regex("^💰 Narxlarni kiritish$"), narx_menu),
                MessageHandler(filters.Regex("^📊 Kunlik hisobot$"), hisobot_menu),
                MessageHandler(filters.Regex("^📈 Statistika$"), statistika),
                MessageHandler(filters.Regex("^🗑 Oxirgi amalni o'chirish$"), oxirgi_amal),
                MessageHandler(filters.Regex("^💼 Savat jami narxi$"), savat_jami),
                MessageHandler(filters.Regex("^📅 Yilni o'zgartirish$"), change_year),
                MessageHandler(filters.Regex("^🗓 Oyni o'zgartirish$"), change_month),
            ],
            CHANGE_YEAR: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_change_year)],
            CHANGE_MONTH: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_change_month)],
            SHOP_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, shop_menu_handler)],
            ADD_SHOP: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_shop)],
            DELETE_SHOP: [MessageHandler(filters.TEXT & ~filters.COMMAND, delete_shop)],
            SHOP_ITEMS_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, shop_items_handler)],
            ADD_ITEM: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_item)],
            DELETE_ITEM: [MessageHandler(filters.TEXT & ~filters.COMMAND, delete_item)],
            BOZOR_SELECT_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_date_selected)],
            BOZOR_SHOP_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_shop_handler)],
            BOZOR_ITEM_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_item_handler)],
            BOZOR_UNIT_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_unit_handler)],
            BOZOR_KG_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_kg_save)],
            BOZOR_DONA_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_dona_save)],
            NARX_SELECT_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, narx_date_selected)],
            NARX_SHOP_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, narx_shop_handler)],
            NARX_ITEM_MENU: [
                MessageHandler(filters.Regex("^(💰|⚖️|⬅️)"), narx_item_action),
                MessageHandler(filters.TEXT & ~filters.COMMAND, narx_item_handler),
            ],
            NARX_UNIT_CHANGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, narx_unit_change)],
            NARX_KG_CHANGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, narx_kg_change)],
            NARX_DONA_CHANGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, narx_dona_change)],
            ENTER_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_price)],
            HISOBOT_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, hisobot_date_handler)],
            OXIRGI_AMAL_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, oxirgi_amal_date)],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
        per_user=True,
        per_chat=True,
    )

    app.add_handler(conv)
    logger.info("🤖 Bot ishga tushdi!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
