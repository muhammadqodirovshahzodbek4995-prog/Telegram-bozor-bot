import os
import asyncio
import psycopg2
import psycopg2.extras
import logging
import threading
from datetime import datetime
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters, ConversationHandler
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

(MAIN_MENU, SHOP_MENU, ADD_SHOP, DELETE_SHOP,
 SHOP_ITEMS_MENU, ADD_ITEM, DELETE_ITEM,
 BOZOR_SELECT_DATE, BOZOR_SHOP_MENU, BOZOR_ITEM_SELECT,
 BOZOR_UNIT_SELECT, BOZOR_KG_SELECT, BOZOR_DONA_SELECT,
 NARX_SELECT_DATE, NARX_SHOP_MENU, NARX_ITEM_MENU, NARX_UNIT_CHANGE,
 NARX_KG_CHANGE, NARX_DONA_CHANGE, ENTER_PRICE,
 HISOBOT_DATE, OXIRGI_AMAL_DATE,
 CHANGE_YEAR, CHANGE_MONTH) = range(24)

MONTHS = ["Yanvar","Fevral","Mart","Aprel","May","Iyun",
          "Iyul","Avgust","Sentabr","Oktabr","Noyabr","Dekabr"]

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def db_exec(query, params=(), fetch=None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        if fetch == 'one': return cur.fetchone()
        if fetch == 'all': return cur.fetchall()
        if fetch == 'val':
            row = cur.fetchone()
            return list(row.values())[0] if row else None
    except Exception as e:
        conn.rollback()
        logger.error(f"DB: {e}")
        raise
    finally:
        conn.close()

def init_db():
    db_exec("""CREATE TABLE IF NOT EXISTS users(
        user_id BIGINT PRIMARY KEY,
        selected_year INT DEFAULT EXTRACT(YEAR FROM NOW())::INT,
        selected_month INT DEFAULT EXTRACT(MONTH FROM NOW())::INT)""")
    db_exec("""CREATE TABLE IF NOT EXISTS shops(
        id SERIAL PRIMARY KEY, user_id BIGINT, name TEXT, UNIQUE(user_id,name))""")
    db_exec("""CREATE TABLE IF NOT EXISTS items(
        id SERIAL PRIMARY KEY, shop_id INT REFERENCES shops(id) ON DELETE CASCADE,
        name TEXT, UNIQUE(shop_id,name))""")
    db_exec("""CREATE TABLE IF NOT EXISTS cart(
        id SERIAL PRIMARY KEY, user_id BIGINT,
        shop_id INT REFERENCES shops(id) ON DELETE CASCADE,
        item_id INT REFERENCES items(id) ON DELETE CASCADE,
        year INT, month INT, day INT, unit TEXT DEFAULT 'dona', quantity FLOAT DEFAULT 1,
        UNIQUE(user_id,item_id,year,month,day))""")
    db_exec("""CREATE TABLE IF NOT EXISTS prices(
        id SERIAL PRIMARY KEY, user_id BIGINT,
        shop_id INT REFERENCES shops(id) ON DELETE CASCADE,
        item_id INT REFERENCES items(id) ON DELETE CASCADE,
        year INT, month INT, day INT, unit TEXT DEFAULT 'dona',
        quantity FLOAT DEFAULT 1, price_per_unit FLOAT, total_price FLOAT,
        created_at TIMESTAMP DEFAULT NOW())""")
    logger.info("DB ready")

def ensure_user(uid):
    now = datetime.now()
    db_exec("INSERT INTO users(user_id,selected_year,selected_month) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
            (uid, now.year, now.month))

def get_user(uid):
    return db_exec("SELECT * FROM users WHERE user_id=%s", (uid,), fetch='one')

def upd_user(uid, field, val):
    db_exec(f"UPDATE users SET {field}=%s WHERE user_id=%s", (val, uid))

async def del_later(ctx, cid, mid, delay=2):
    await asyncio.sleep(delay)
    try: await ctx.bot.delete_message(cid, mid)
    except: pass

def mmk():
    return ReplyKeyboardMarkup([
        ["🛒 Ro'yxat kiritish","🏪 Bozorga ro'yxat"],
        ["💰 Narxlarni kiritish","📊 Kunlik hisobot"],
        ["📈 Statistika","🗑 Oxirgi amalni o'chirish"],
        ["💼 Savat jami narxi"],
        ["📅 Yilni o'zgartirish","🗓 Oyni o'zgartirish"]
    ], resize_keyboard=True)

def bk(): return ReplyKeyboardMarkup([["⬅️ Orqaga"]], resize_keyboard=True)

async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    ensure_user(uid)
    user = get_user(uid)
    yr, mn = user['selected_year'], MONTHS[user['selected_month']-1]
    await u.message.reply_text(f"👋 Xush kelibsiz!\n📅 {mn} {yr}", reply_markup=mmk())
    return MAIN_MENU

async def go_main(u, c):
    user = get_user(u.effective_user.id)
    yr, mn = user['selected_year'], MONTHS[user['selected_month']-1]
    await u.message.reply_text(f"🏠 {mn} {yr}", reply_markup=mmk())
    return MAIN_MENU

# YIL OY
async def change_year(u, c):
    user = get_user(u.effective_user.id)
    cur = user['selected_year']
    kb = ReplyKeyboardMarkup([[f"◀️ {cur-1}",f"✅ {cur}",f"▶️ {cur+1}"],["⬅️ Orqaga"]], resize_keyboard=True)
    await u.message.reply_text(f"Yilni tanlang (hozir: {cur}):", reply_markup=kb)
    return CHANGE_YEAR

async def hcyear(u, c):
    txt = u.message.text; uid = u.effective_user.id
    if txt == "⬅️ Orqaga": return await go_main(u, c)
    user = get_user(uid); cur = user['selected_year']
    new = cur-1 if str(cur-1) in txt else (cur+1 if str(cur+1) in txt else cur)
    upd_user(uid,'selected_year',new)
    m = await u.message.reply_text(f"✅ {new} yil!")
    asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
    return await go_main(u, c)

async def change_month(u, c):
    kb = [MONTHS[i:i+3] for i in range(0,12,3)] + [["⬅️ Orqaga"]]
    await u.message.reply_text("Oyni tanlang:", reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))
    return CHANGE_MONTH

async def hcmonth(u, c):
    txt = u.message.text; uid = u.effective_user.id
    if txt == "⬅️ Orqaga": return await go_main(u, c)
    if txt in MONTHS:
        upd_user(uid,'selected_month',MONTHS.index(txt)+1)
        m = await u.message.reply_text(f"✅ {txt}!")
        asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
    return await go_main(u, c)

# ROYXAT
async def royxat_menu(u, c):
    uid = u.effective_user.id
    shops = db_exec("SELECT * FROM shops WHERE user_id=%s ORDER BY name",(uid,),fetch='all') or []
    btns = [[s['name']] for s in shops]
    btns += [["➕ Do'kon qo'shish","🗑 Do'kon o'chirish"],["⬅️ Orqaga"]]
    await u.message.reply_text("🏪 Do'konlar:", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return SHOP_MENU

async def shop_menu_h(u, c):
    txt = u.message.text; uid = u.effective_user.id
    if txt == "⬅️ Orqaga": return await go_main(u,c)
    if txt == "➕ Do'kon qo'shish":
        await u.message.reply_text("Do'kon nomini kiriting:", reply_markup=bk())
        return ADD_SHOP
    if txt == "🗑 Do'kon o'chirish":
        shops = db_exec("SELECT * FROM shops WHERE user_id=%s",(uid,),fetch='all') or []
        if not shops:
            m = await u.message.reply_text("❌ Do'konlar yo'q!")
            asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
            return SHOP_MENU
        btns = [[s['name']] for s in shops]+[["⬅️ Orqaga"]]
        await u.message.reply_text("O'chirish:", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
        return DELETE_SHOP
    shop = db_exec("SELECT * FROM shops WHERE user_id=%s AND name=%s",(uid,txt),fetch='one')
    if shop:
        c.user_data['csi'] = shop['id']; c.user_data['csn'] = shop['name']
        return await show_items(u,c,shop['id'],shop['name'])
    return SHOP_MENU

async def add_shop(u, c):
    txt = u.message.text; uid = u.effective_user.id
    if txt == "⬅️ Orqaga": return await royxat_menu(u,c)
    try:
        db_exec("INSERT INTO shops(user_id,name) VALUES(%s,%s)",(uid,txt))
        m = await u.message.reply_text(f"✅ '{txt}' qo'shildi!")
    except:
        m = await u.message.reply_text("❌ Mavjud!")
    asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
    return await royxat_menu(u,c)

async def del_shop(u, c):
    txt = u.message.text; uid = u.effective_user.id
    if txt == "⬅️ Orqaga": return await royxat_menu(u,c)
    db_exec("DELETE FROM shops WHERE user_id=%s AND name=%s",(uid,txt))
    m = await u.message.reply_text(f"✅ '{txt}' o'chirildi!")
    asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
    return await royxat_menu(u,c)

async def show_items(u, c, sid, sname):
    items = db_exec("SELECT * FROM items WHERE shop_id=%s ORDER BY name",(sid,),fetch='all') or []
    btns = [["➕ Tovar qo'shish","🗑 Tovarni o'chirish"],["⬅️ Orqaga"]]
    await u.message.reply_text(f"🏪 {sname} ({len(items)} tovar)",reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return SHOP_ITEMS_MENU

async def shop_items_h(u, c):
    txt = u.message.text; sid = c.user_data.get('csi'); sname = c.user_data.get('csn')
    if txt == "⬅️ Orqaga": return await royxat_menu(u,c)
    if txt == "➕ Tovar qo'shish":
        await u.message.reply_text("Tovar nomini kiriting:", reply_markup=bk())
        return ADD_ITEM
    if txt == "🗑 Tovarni o'chirish":
        items = db_exec("SELECT * FROM items WHERE shop_id=%s",(sid,),fetch='all') or []
        if not items:
            m = await u.message.reply_text("❌ Tovar yo'q!")
            asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
            return SHOP_ITEMS_MENU
        btns = [[i['name']] for i in items]+[["⬅️ Orqaga"]]
        await u.message.reply_text("O'chirish:", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
        return DELETE_ITEM
    return SHOP_ITEMS_MENU

async def add_item(u, c):
    txt = u.message.text; sid = c.user_data.get('csi'); sname = c.user_data.get('csn')
    if txt == "⬅️ Orqaga": return await show_items(u,c,sid,sname)
    try:
        db_exec("INSERT INTO items(shop_id,name) VALUES(%s,%s)",(sid,txt))
        m = await u.message.reply_text(f"✅ '{txt}' qo'shildi!")
    except:
        m = await u.message.reply_text("❌ Mavjud!")
    asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
    return await show_items(u,c,sid,sname)

async def del_item(u, c):
    txt = u.message.text; sid = c.user_data.get('csi'); sname = c.user_data.get('csn')
    if txt == "⬅️ Orqaga": return await show_items(u,c,sid,sname)
    db_exec("DELETE FROM items WHERE shop_id=%s AND name=%s",(sid,txt))
    m = await u.message.reply_text(f"✅ '{txt}' o'chirildi!")
    asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
    return await show_items(u,c,sid,sname)

# BOZOR
async def bozor_menu(u, c):
    uid = u.effective_user.id; user = get_user(uid)
    yr,mo = user['selected_year'],user['selected_month']
    btns = [list(map(str,range(i,min(i+7,32)))) for i in range(1,32,7)]+[["⬅️ Orqaga"]]
    await u.message.reply_text(f"📅 {MONTHS[mo-1]} {yr}\nSanani tanlang:",
        reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return BOZOR_SELECT_DATE

async def bozor_date(u, c):
    txt = u.message.text; uid = u.effective_user.id
    if txt == "⬅️ Orqaga": return await go_main(u,c)
    try:
        d = int(txt); user = get_user(uid)
        c.user_data.update({'bd':d,'by':user['selected_year'],'bm':user['selected_month']})
        return await show_bozor_shops(u,c)
    except: return BOZOR_SELECT_DATE

async def show_bozor_shops(u, c):
    uid = u.effective_user.id; d,yr,mo = c.user_data['bd'],c.user_data['by'],c.user_data['bm']
    shops = db_exec("SELECT * FROM shops WHERE user_id=%s ORDER BY name",(uid,),fetch='all') or []
    cnt = db_exec("SELECT COUNT(*) FROM cart WHERE user_id=%s AND year=%s AND month=%s AND day=%s",
        (uid,yr,mo,d),fetch='val') or 0
    btns = [[s['name']] for s in shops]
    btns += [["🗑 Savatdan o'chirish",f"🛒 Savat ({cnt})"],["⬅️ Orqaga"]]
    await u.message.reply_text(f"🏪 {d}.{mo:02d}.{yr} — Savat: {cnt}",
        reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return BOZOR_SHOP_MENU

async def bozor_shop_h(u, c):
    txt = u.message.text; uid = u.effective_user.id
    d,yr,mo = c.user_data['bd'],c.user_data['by'],c.user_data['bm']
    if txt == "⬅️ Orqaga": return await bozor_menu(u,c)
    if txt.startswith("🛒"): return await show_cart(u,c)
    if txt == "🗑 Savatdan o'chirish":
        rows = db_exec("""SELECT c.id,i.name iname,s.name sname,c.quantity,c.unit
            FROM cart c JOIN items i ON c.item_id=i.id JOIN shops s ON c.shop_id=s.id
            WHERE c.user_id=%s AND c.year=%s AND c.month=%s AND c.day=%s ORDER BY s.name,i.name""",
            (uid,yr,mo,d),fetch='all') or []
        if not rows:
            m = await u.message.reply_text("❌ Savat bo'sh!")
            asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
            return await show_bozor_shops(u,c)
        c.user_data['dfc'] = True
        btns = [[f"🗑 {r['iname']} ({r['quantity']}{r['unit']}) [{r['sname']}]"] for r in rows]+[["⬅️ Orqaga"]]
        await u.message.reply_text("O'chirish:", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
        return BOZOR_ITEM_SELECT
    shop = db_exec("SELECT * FROM shops WHERE user_id=%s AND name=%s",(uid,txt),fetch='one')
    if not shop: return BOZOR_SHOP_MENU
    c.user_data['bsi'] = shop['id']; c.user_data['bsn'] = shop['name']
    items = db_exec("SELECT * FROM items WHERE shop_id=%s ORDER BY name",(shop['id'],),fetch='all') or []
    cart_ns = [r['name'] for r in (db_exec("""SELECT i.name FROM cart c JOIN items i ON c.item_id=i.id
        WHERE c.user_id=%s AND c.shop_id=%s AND c.year=%s AND c.month=%s AND c.day=%s""",
        (uid,shop['id'],yr,mo,d),fetch='all') or [])]
    btns = [[("✅ " if i['name'] in cart_ns else "")+i['name']] for i in items]
    btns += [["➕ Tovar qo'shish (bu do'kon)"],["⬅️ Orqaga"]]
    await u.message.reply_text(f"🏪 {shop['name']}", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return BOZOR_ITEM_SELECT

async def bozor_item_h(u, c):
    txt = u.message.text; uid = u.effective_user.id
    d,yr,mo = c.user_data['bd'],c.user_data['by'],c.user_data['bm']
    sid = c.user_data.get('bsi'); sname = c.user_data.get('bsn')
    if txt == "⬅️ Orqaga":
        c.user_data.pop('dfc',None); c.user_data.pop('aci',None)
        return await show_bozor_shops(u,c)
    if c.user_data.get('dfc') and txt.startswith("🗑 "):
        iname = txt.split("🗑 ")[1].split(" (")[0]
        db_exec("""DELETE FROM cart WHERE user_id=%s AND year=%s AND month=%s AND day=%s
            AND item_id=(SELECT i.id FROM items i JOIN shops s ON i.shop_id=s.id
            WHERE i.name=%s AND s.user_id=%s LIMIT 1)""",(uid,yr,mo,d,iname,uid))
        c.user_data.pop('dfc',None)
        m = await u.message.reply_text(f"✅ '{iname}' o'chirildi!")
        asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
        return await show_bozor_shops(u,c)
    if txt == "➕ Tovar qo'shish (bu do'kon)":
        await u.message.reply_text(f"🏪 {sname}\nTovar nomini kiriting:", reply_markup=bk())
        c.user_data['aci'] = True; return BOZOR_ITEM_SELECT
    if c.user_data.get('aci'):
        try: db_exec("INSERT INTO items(shop_id,name) VALUES(%s,%s)",(sid,txt))
        except: pass
        item = db_exec("SELECT * FROM items WHERE shop_id=%s AND name=%s",(sid,txt),fetch='one')
        c.user_data['bii'] = item['id']; c.user_data['bin'] = txt
        c.user_data.pop('aci',None); return await ask_unit(u,c)
    iname = txt.replace("✅ ","")
    item = db_exec("SELECT * FROM items WHERE shop_id=%s AND name=%s",(sid,iname),fetch='one')
    if not item: return BOZOR_ITEM_SELECT
    c.user_data['bii'] = item['id']; c.user_data['bin'] = iname
    return await ask_unit(u,c)

async def ask_unit(u, c):
    iname = c.user_data.get('bin'); uid = u.effective_user.id
    d,yr,mo,iid = c.user_data['bd'],c.user_data['by'],c.user_data['bm'],c.user_data['bii']
    ex = db_exec("SELECT * FROM cart WHERE user_id=%s AND item_id=%s AND year=%s AND month=%s AND day=%s",
        (uid,iid,yr,mo,d),fetch='one')
    btns = ([[f"✅ {ex['unit']} ({ex['quantity']})"]] if ex else []) + [["kg","dona"],["⬅️ Orqaga"]]
    await u.message.reply_text(f"📦 {iname}\nBirlikni tanlang:", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return BOZOR_UNIT_SELECT

async def bozor_unit_h(u, c):
    txt = u.message.text
    if txt == "⬅️ Orqaga": return await show_bozor_shops(u,c)
    if "kg" in txt:
        c.user_data['bu'] = 'kg'; c.user_data['bq'] = 0.5
        btns = [["-10","-0.5","0.5kg","+0.5","+10"],["✅ Saqlash"],["⬅️ Orqaga"]]
        await u.message.reply_text("⚖️ Necha kg?", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
        return BOZOR_KG_SELECT
    if "dona" in txt:
        c.user_data['bu'] = 'dona'; c.user_data['bq'] = 1
        btns = [["-1","1dona","+1"],["✅ Saqlash"],["⬅️ Orqaga"]]
        await u.message.reply_text("🔢 Nechta?", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
        return BOZOR_DONA_SELECT
    return BOZOR_UNIT_SELECT

async def bozor_kg_h(u, c):
    txt = u.message.text
    if txt == "⬅️ Orqaga": return await ask_unit(u,c)
    if txt == "✅ Saqlash": return await save_cart(u,c)
    q = c.user_data.get('bq',0.5)
    if txt=="+0.5": q=round(q+0.5,1)
    elif txt=="-0.5": q=max(0.5,round(q-0.5,1))
    elif txt=="+10": q=round(q+10,1)
    elif txt=="-10": q=max(0.5,round(q-10,1))
    c.user_data['bq'] = q
    btns=[["-10","-0.5",f"{q}kg","+0.5","+10"],["✅ Saqlash"],["⬅️ Orqaga"]]
    await u.message.reply_text(f"⚖️ {q} kg", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return BOZOR_KG_SELECT

async def bozor_dona_h(u, c):
    txt = u.message.text
    if txt == "⬅️ Orqaga": return await ask_unit(u,c)
    if txt == "✅ Saqlash": return await save_cart(u,c)
    q = c.user_data.get('bq',1)
    if txt=="+1": q+=1
    elif txt=="-1": q=max(1,q-1)
    c.user_data['bq'] = q
    btns=[["-1",f"{q}dona","+1"],["✅ Saqlash"],["⬅️ Orqaga"]]
    await u.message.reply_text(f"🔢 {q} dona", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return BOZOR_DONA_SELECT

async def save_cart(u, c):
    uid = u.effective_user.id
    d,yr,mo = c.user_data['bd'],c.user_data['by'],c.user_data['bm']
    sid,iid,iname = c.user_data['bsi'],c.user_data['bii'],c.user_data['bin']
    unit,qty = c.user_data.get('bu','dona'),c.user_data.get('bq',1)
    db_exec("""INSERT INTO cart(user_id,shop_id,item_id,year,month,day,unit,quantity)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,item_id,year,month,day)
        DO UPDATE SET unit=%s,quantity=%s""",(uid,sid,iid,yr,mo,d,unit,qty,unit,qty))
    m = await u.message.reply_text(f"✅ '{iname}' {qty}{unit} savatga!")
    asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
    return await show_bozor_shops(u,c)

async def show_cart(u, c):
    uid = u.effective_user.id; d,yr,mo = c.user_data['bd'],c.user_data['by'],c.user_data['bm']
    rows = db_exec("""SELECT c.*,i.name iname,s.name sname FROM cart c
        JOIN items i ON c.item_id=i.id JOIN shops s ON c.shop_id=s.id
        WHERE c.user_id=%s AND c.year=%s AND c.month=%s AND c.day=%s ORDER BY s.name,i.name""",
        (uid,yr,mo,d),fetch='all') or []
    if not rows:
        m = await u.message.reply_text("🛒 Bo'sh!")
        asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
        return await show_bozor_shops(u,c)
    txt = f"🛒 {d}.{mo:02d}.{yr}\n"; cs = ""
    for r in rows:
        if r['sname']!=cs: cs=r['sname']; txt+=f"\n🏪 {cs}:\n"
        txt+=f"  📦 {r['iname']} — {r['quantity']}{r['unit']}\n"
    await u.message.reply_text(txt, reply_markup=ReplyKeyboardMarkup([["⬅️ Orqaga"]],resize_keyboard=True))
    return BOZOR_SHOP_MENU

# NARX
async def narx_menu(u, c):
    uid = u.effective_user.id; user = get_user(uid)
    yr,mo = user['selected_year'],user['selected_month']
    days = db_exec("SELECT DISTINCT day FROM cart WHERE user_id=%s AND year=%s AND month=%s ORDER BY day",
        (uid,yr,mo),fetch='all') or []
    if not days:
        m = await u.message.reply_text("❌ Savat to'ldirilmagan!")
        asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
        return await go_main(u,c)
    btns = [[str(d['day'])] for d in days]+[["⬅️ Orqaga"]]
    await u.message.reply_text(f"📅 {MONTHS[mo-1]} {yr}", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return NARX_SELECT_DATE

async def narx_date(u, c):
    txt = u.message.text; uid = u.effective_user.id
    if txt == "⬅️ Orqaga": return await go_main(u,c)
    try:
        d = int(txt); user = get_user(uid)
        c.user_data.update({'nd':d,'ny':user['selected_year'],'nm':user['selected_month']})
        return await show_narx_shops(u,c)
    except: return NARX_SELECT_DATE

async def show_narx_shops(u, c):
    uid = u.effective_user.id; d,yr,mo = c.user_data['nd'],c.user_data['ny'],c.user_data['nm']
    shops = db_exec("""SELECT DISTINCT s.id,s.name FROM cart c JOIN shops s ON c.shop_id=s.id
        WHERE c.user_id=%s AND c.year=%s AND c.month=%s AND c.day=%s ORDER BY s.name""",
        (uid,yr,mo,d),fetch='all') or []
    btns = [[s['name']] for s in shops]+[["⬅️ Orqaga"]]
    await u.message.reply_text(f"📅 {d}.{mo:02d}.{yr}", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return NARX_SHOP_MENU

async def narx_shop_h(u, c):
    txt = u.message.text; uid = u.effective_user.id
    if txt == "⬅️ Orqaga": return await narx_menu(u,c)
    shop = db_exec("SELECT * FROM shops WHERE user_id=%s AND name=%s",(uid,txt),fetch='one')
    if not shop: return NARX_SHOP_MENU
    c.user_data['nsi'] = shop['id']; c.user_data['nsn'] = shop['name']
    return await show_narx_items(u,c)

async def show_narx_items(u, c):
    uid = u.effective_user.id; d,yr,mo = c.user_data['nd'],c.user_data['ny'],c.user_data['nm']
    sid,sname = c.user_data['nsi'],c.user_data['nsn']
    citems = db_exec("""SELECT c.*,i.name iname FROM cart c JOIN items i ON c.item_id=i.id
        WHERE c.user_id=%s AND c.shop_id=%s AND c.year=%s AND c.month=%s AND c.day=%s ORDER BY i.name""",
        (uid,sid,yr,mo,d),fetch='all') or []
    pids = {r['item_id'] for r in (db_exec("SELECT item_id FROM prices WHERE user_id=%s AND shop_id=%s AND year=%s AND month=%s AND day=%s",
        (uid,sid,yr,mo,d),fetch='all') or [])}
    btns = [[("✅ " if ci['item_id'] in pids else "")+f"{ci['iname']} ({ci['quantity']}{ci['unit']})"] for ci in citems]
    btns.append(["⬅️ Orqaga"])
    await u.message.reply_text(f"🏪 {sname} — {d}.{mo:02d}.{yr}",
        reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return NARX_ITEM_MENU

async def narx_item_h(u, c):
    txt = u.message.text; uid = u.effective_user.id
    sid = c.user_data.get('nsi'); d,yr,mo = c.user_data['nd'],c.user_data['ny'],c.user_data['nm']
    if txt == "⬅️ Orqaga": return await show_narx_shops(u,c)
    if txt.startswith("💰") or txt.startswith("⚖️"): return await narx_action(u,c)
    clean = txt.replace("✅ ","").split(" (")[0]
    item = db_exec("SELECT i.* FROM items i WHERE i.shop_id=%s AND i.name=%s",(sid,clean),fetch='one')
    if not item: return NARX_ITEM_MENU
    ci = db_exec("SELECT * FROM cart WHERE user_id=%s AND item_id=%s AND year=%s AND month=%s AND day=%s",
        (uid,item['id'],yr,mo,d),fetch='one')
    lp = db_exec("SELECT price_per_unit,unit FROM prices WHERE user_id=%s AND item_id=%s ORDER BY year DESC,month DESC,day DESC LIMIT 1",
        (uid,item['id']),fetch='one')
    c.user_data['nii'] = item['id']; c.user_data['nin'] = clean
    if ci: c.user_data['nu'] = ci['unit']; c.user_data['nq'] = ci['quantity']
    qty = ci['quantity'] if ci else 1; unit = ci['unit'] if ci else 'dona'
    lt = f"\n⬇️ Oxirgi: {lp['price_per_unit']:,.0f} so'm/{lp['unit']}" if lp else ""
    btns = [[f"💰 Narxini kiritish ({qty}{unit})"],["⚖️ kg/dona o'zgartirish"],["⬅️ Orqaga"]]
    await u.message.reply_text(f"📦 {clean}\n{qty}{unit}{lt}", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return NARX_ITEM_MENU

async def narx_action(u, c):
    txt = u.message.text; uid = u.effective_user.id
    iname,qty,unit = c.user_data.get('nin'),c.user_data.get('nq',1),c.user_data.get('nu','dona')
    if txt == "⬅️ Orqaga": return await show_narx_items(u,c)
    if txt.startswith("💰"):
        lp = db_exec("SELECT price_per_unit FROM prices WHERE user_id=%s AND item_id=%s ORDER BY year DESC,month DESC,day DESC LIMIT 1",
            (uid,c.user_data.get('nii')),fetch='one')
        lt = f"\n⬇️ Oxirgi: {lp['price_per_unit']:,.0f} so'm" if lp else ""
        await u.message.reply_text(f"📦 {iname} — {qty}{unit}\n1{unit} narxini kiriting:{lt}", reply_markup=bk())
        return ENTER_PRICE
    if txt.startswith("⚖️"):
        await u.message.reply_text("Birlikni tanlang:", reply_markup=ReplyKeyboardMarkup([["kg","dona"],["⬅️ Orqaga"]],resize_keyboard=True))
        return NARX_UNIT_CHANGE
    return NARX_ITEM_MENU

async def narx_unit_ch(u, c):
    txt = u.message.text
    if txt == "⬅️ Orqaga": return await show_narx_items(u,c)
    if txt in ["kg","dona"]:
        c.user_data['nu'] = txt
        if txt=="kg":
            c.user_data['nq']=0.5; btns=[["-10","-0.5","0.5kg","+0.5","+10"],["✅ Saqlash"],["⬅️ Orqaga"]]
            await u.message.reply_text("⚖️ Necha kg?",reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True)); return NARX_KG_CHANGE
        else:
            c.user_data['nq']=1; btns=[["-1","1dona","+1"],["✅ Saqlash"],["⬅️ Orqaga"]]
            await u.message.reply_text("🔢 Nechta?",reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True)); return NARX_DONA_CHANGE
    return NARX_UNIT_CHANGE

async def narx_kg_ch(u, c):
    txt = u.message.text
    if txt == "⬅️ Orqaga": return await show_narx_items(u,c)
    q = c.user_data.get('nq',0.5)
    if txt=="✅ Saqlash":
        await upd_cart_unit(u.effective_user.id,c,'kg',q); return await show_narx_items(u,c)
    if txt=="+0.5": q=round(q+0.5,1)
    elif txt=="-0.5": q=max(0.5,round(q-0.5,1))
    elif txt=="+10": q=round(q+10,1)
    elif txt=="-10": q=max(0.5,round(q-10,1))
    c.user_data['nq']=q
    btns=[["-10","-0.5",f"{q}kg","+0.5","+10"],["✅ Saqlash"],["⬅️ Orqaga"]]
    await u.message.reply_text(f"⚖️ {q}kg",reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return NARX_KG_CHANGE

async def narx_dona_ch(u, c):
    txt = u.message.text
    if txt == "⬅️ Orqaga": return await show_narx_items(u,c)
    q = c.user_data.get('nq',1)
    if txt=="✅ Saqlash":
        await upd_cart_unit(u.effective_user.id,c,'dona',q); return await show_narx_items(u,c)
    if txt=="+1": q+=1
    elif txt=="-1": q=max(1,q-1)
    c.user_data['nq']=q
    btns=[["-1",f"{q}dona","+1"],["✅ Saqlash"],["⬅️ Orqaga"]]
    await u.message.reply_text(f"🔢 {q}dona",reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return NARX_DONA_CHANGE

async def upd_cart_unit(uid, c, unit, qty):
    iid = c.user_data.get('nii'); d,yr,mo = c.user_data['nd'],c.user_data['ny'],c.user_data['nm']
    db_exec("UPDATE cart SET unit=%s,quantity=%s WHERE user_id=%s AND item_id=%s AND year=%s AND month=%s AND day=%s",
        (unit,qty,uid,iid,yr,mo,d))
    c.user_data['nu']=unit; c.user_data['nq']=qty

async def enter_price(u, c):
    txt = u.message.text; uid = u.effective_user.id
    if txt == "⬅️ Orqaga": return await show_narx_items(u,c)
    try:
        price = float(txt.replace(",","").replace(" ",""))
        iid,iname,sid = c.user_data['nii'],c.user_data['nin'],c.user_data['nsi']
        d,yr,mo = c.user_data['nd'],c.user_data['ny'],c.user_data['nm']
        unit,qty = c.user_data.get('nu','dona'),c.user_data.get('nq',1)
        total = price*qty
        db_exec("""INSERT INTO prices(user_id,shop_id,item_id,year,month,day,unit,quantity,price_per_unit,total_price)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (uid,sid,iid,yr,mo,d,unit,qty,price,total))
        m = await u.message.reply_text(f"✅ {iname}: {qty}{unit}×{price:,.0f}={total:,.0f} so'm!")
        asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
        return await show_narx_items(u,c)
    except:
        m = await u.message.reply_text("❌ Faqat raqam!")
        asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
        return ENTER_PRICE

# HISOBOT
async def hisobot_menu(u, c):
    uid = u.effective_user.id; user = get_user(uid); yr,mo = user['selected_year'],user['selected_month']
    dates = db_exec("SELECT DISTINCT day FROM prices WHERE user_id=%s AND year=%s AND month=%s ORDER BY day",
        (uid,yr,mo),fetch='all') or []
    if not dates:
        m = await u.message.reply_text("❌ Narx kiritilmagan!")
        asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
        return await go_main(u,c)
    btns = [[f"{d['day']:02d}.{mo:02d}.{yr}"] for d in dates]+[["⬅️ Orqaga"]]
    await u.message.reply_text("📅 Sana:", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return HISOBOT_DATE

async def hisobot_date_h(u, c):
    txt = u.message.text; uid = u.effective_user.id
    if txt == "⬅️ Orqaga": return await go_main(u,c)
    try:
        p=txt.split("."); d,mo,yr=int(p[0]),int(p[1]),int(p[2])
    except: return HISOBOT_DATE
    rows = db_exec("""SELECT p.*,i.name iname,s.name sname FROM prices p
        JOIN items i ON p.item_id=i.id JOIN shops s ON p.shop_id=s.id
        WHERE p.user_id=%s AND p.year=%s AND p.month=%s AND p.day=%s ORDER BY s.name,i.name""",
        (uid,yr,mo,d),fetch='all') or []
    if not rows:
        m = await u.message.reply_text("❌ Ma'lumot yo'q!")
        asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id)); return HISOBOT_DATE
    txt2 = f"📊 {d:02d}.{mo:02d}.{yr}\n\n"; cs=""; st=0; gt=0
    for r in rows:
        if r['sname']!=cs:
            if cs: txt2+=f"📦 {cs} jami: {st:,.0f}\n\n"
            cs=r['sname']; st=0; txt2+=f"🏪 {cs}:\n"
        txt2+=f"  • {r['iname']} {r['quantity']}{r['unit']} — {r['total_price']:,.0f} so'm\n"
        st+=r['total_price']; gt+=r['total_price']
    txt2+=f"📦 {cs} jami: {st:,.0f}\n\n💰 JAMI: {gt:,.0f} so'm"
    await u.message.reply_text(txt2, reply_markup=ReplyKeyboardMarkup([["⬅️ Orqaga"]],resize_keyboard=True))
    return HISOBOT_DATE

# STATISTIKA
async def statistika(u, c):
    uid = u.effective_user.id; user = get_user(uid); yr,mo = user['selected_year'],user['selected_month']
    dates = db_exec("SELECT DISTINCT day FROM prices WHERE user_id=%s AND year=%s AND month=%s ORDER BY day DESC LIMIT 3",
        (uid,yr,mo),fetch='all') or []
    if not dates:
        m = await u.message.reply_text("❌ Ma'lumot yetarli emas!")
        asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id)); return await go_main(u,c)
    days = sorted([d['day'] for d in dates]); ld = days[-1]; pdays = days[:-1]
    latest = db_exec("""SELECT p.*,i.name iname,s.name sname FROM prices p
        JOIN items i ON p.item_id=i.id JOIN shops s ON p.shop_id=s.id
        WHERE p.user_id=%s AND p.year=%s AND p.month=%s AND p.day=%s ORDER BY s.name,i.name""",
        (uid,yr,mo,ld),fetch='all') or []
    txt = f"📈 Statistika — {ld:02d}.{mo:02d}.{yr}\n"; cs=""; rose=[]; fell=[]
    for r in latest:
        if r['sname']!=cs: cs=r['sname']; txt+=f"\n🏪 {cs}:\n"
        cp=r['price_per_unit']; pinfo=[]; trend="✅"
        for pd in pdays:
            prev=db_exec("SELECT price_per_unit FROM prices WHERE user_id=%s AND item_id=%s AND year=%s AND month=%s AND day=%s",
                (uid,r['item_id'],yr,mo,pd),fetch='one')
            if prev:
                pinfo.append(f"{pd:02d}.{mo:02d}.{yr}:{prev['price_per_unit']:,.0f}")
                if cp>prev['price_per_unit']: trend="❌↗️"
                elif cp<prev['price_per_unit'] and trend!="❌↗️": trend="⭕️↘️"
        if trend=="❌↗️": rose.append(r['iname'])
        elif trend=="⭕️↘️": fell.append(r['iname'])
        pi = " / ".join(pinfo) if pinfo else "—"
        txt+=f"  {r['iname']} {trend} {cp:,.0f} so'm | {pi}\n"
    txt+="\n📊 Xulosa:\n"
    if rose: txt+=f"❌↗️ Oshgan: {', '.join(rose)}\n"
    if fell: txt+=f"⭕️↘️ Tushgan: {', '.join(fell)}\n"
    if not rose and not fell: txt+="✅ Barqaror\n"
    await u.message.reply_text(txt, reply_markup=mmk()); return MAIN_MENU

# OXIRGI AMAL
async def oxirgi_amal(u, c):
    uid = u.effective_user.id; user = get_user(uid); yr,mo = user['selected_year'],user['selected_month']
    dates = db_exec("SELECT DISTINCT day FROM prices WHERE user_id=%s AND year=%s AND month=%s ORDER BY day",
        (uid,yr,mo),fetch='all') or []
    if not dates:
        m = await u.message.reply_text("❌ Ma'lumot yo'q!")
        asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id)); return await go_main(u,c)
    btns = [[f"{d['day']:02d}.{mo:02d}.{yr}"] for d in dates]+[["⬅️ Orqaga"]]
    await u.message.reply_text("🗑 Sana:", reply_markup=ReplyKeyboardMarkup(btns,resize_keyboard=True))
    return OXIRGI_AMAL_DATE

async def oxirgi_amal_date_h(u, c):
    txt = u.message.text; uid = u.effective_user.id
    if txt == "⬅️ Orqaga": return await go_main(u,c)
    try:
        p=txt.split("."); d,mo,yr=int(p[0]),int(p[1]),int(p[2])
    except: return OXIRGI_AMAL_DATE
    last = db_exec("""SELECT p.id,i.name iname FROM prices p JOIN items i ON p.item_id=i.id
        WHERE p.user_id=%s AND p.year=%s AND p.month=%s AND p.day=%s ORDER BY p.created_at DESC LIMIT 1""",
        (uid,yr,mo,d),fetch='one')
    if not last:
        m = await u.message.reply_text("❌ Ma'lumot yo'q!")
        asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id)); return OXIRGI_AMAL_DATE
    db_exec("DELETE FROM prices WHERE id=%s",(last['id'],))
    m = await u.message.reply_text(f"✅ '{last['iname']}' narxi o'chirildi!")
    asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id))
    return OXIRGI_AMAL_DATE

# SAVAT JAMI
async def savat_jami(u, c):
    uid = u.effective_user.id; user = get_user(uid); yr,mo = user['selected_year'],user['selected_month']
    days = db_exec("SELECT DISTINCT day FROM cart WHERE user_id=%s AND year=%s AND month=%s ORDER BY day",
        (uid,yr,mo),fetch='all') or []
    if not days:
        m = await u.message.reply_text("❌ Savat bo'sh!")
        asyncio.create_task(del_later(c,u.effective_chat.id,m.message_id)); return MAIN_MENU
    txt = f"💼 Savat Jami — {MONTHS[mo-1]} {yr}\n\n"
    for d in days:
        day=d['day']
        items = db_exec("""SELECT c.quantity,
            (SELECT price_per_unit FROM prices p2 WHERE p2.user_id=c.user_id AND p2.item_id=c.item_id
             ORDER BY p2.year DESC,p2.month DESC,p2.day DESC LIMIT 1) lp
            FROM cart c WHERE c.user_id=%s AND c.year=%s AND c.month=%s AND c.day=%s""",
            (uid,yr,mo,day),fetch='all') or []
        total = sum((r['lp'] or 0)*r['quantity'] for r in items)
        txt+=f"📅 {day:02d}.{mo:02d}.{yr}: {total:,.0f} so'm\n"
    await u.message.reply_text(txt, reply_markup=mmk()); return MAIN_MENU

# HEALTH
def run_health():
    import http.server, socketserver
    port = int(os.getenv("PORT",10000))
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
        def log_message(self,*a): pass
    with socketserver.TCPServer(("",port),H) as s:
        logger.info(f"✅ Health :{port}"); s.serve_forever()

def main():
    init_db()
    threading.Thread(target=run_health,daemon=True).start()
    app = Application.builder().token(TOKEN).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler("start",start)],
        states={
            MAIN_MENU:[
                MessageHandler(filters.Regex("^🛒 Ro'yxat kiritish$"),royxat_menu),
                MessageHandler(filters.Regex("^🏪 Bozorga ro'yxat$"),bozor_menu),
                MessageHandler(filters.Regex("^💰 Narxlarni kiritish$"),narx_menu),
                MessageHandler(filters.Regex("^📊 Kunlik hisobot$"),hisobot_menu),
                MessageHandler(filters.Regex("^📈 Statistika$"),statistika),
                MessageHandler(filters.Regex("^🗑 Oxirgi amalni o'chirish$"),oxirgi_amal),
                MessageHandler(filters.Regex("^💼 Savat jami narxi$"),savat_jami),
                MessageHandler(filters.Regex("^📅 Yilni o'zgartirish$"),change_year),
                MessageHandler(filters.Regex("^🗓 Oyni o'zgartirish$"),change_month),
            ],
            CHANGE_YEAR:[MessageHandler(filters.TEXT&~filters.COMMAND,hcyear)],
            CHANGE_MONTH:[MessageHandler(filters.TEXT&~filters.COMMAND,hcmonth)],
            SHOP_MENU:[MessageHandler(filters.TEXT&~filters.COMMAND,shop_menu_h)],
            ADD_SHOP:[MessageHandler(filters.TEXT&~filters.COMMAND,add_shop)],
            DELETE_SHOP:[MessageHandler(filters.TEXT&~filters.COMMAND,del_shop)],
            SHOP_ITEMS_MENU:[MessageHandler(filters.TEXT&~filters.COMMAND,shop_items_h)],
            ADD_ITEM:[MessageHandler(filters.TEXT&~filters.COMMAND,add_item)],
            DELETE_ITEM:[MessageHandler(filters.TEXT&~filters.COMMAND,del_item)],
            BOZOR_SELECT_DATE:[MessageHandler(filters.TEXT&~filters.COMMAND,bozor_date)],
            BOZOR_SHOP_MENU:[MessageHandler(filters.TEXT&~filters.COMMAND,bozor_shop_h)],
            BOZOR_ITEM_SELECT:[MessageHandler(filters.TEXT&~filters.COMMAND,bozor_item_h)],
            BOZOR_UNIT_SELECT:[MessageHandler(filters.TEXT&~filters.COMMAND,bozor_unit_h)],
            BOZOR_KG_SELECT:[MessageHandler(filters.TEXT&~filters.COMMAND,bozor_kg_h)],
            BOZOR_DONA_SELECT:[MessageHandler(filters.TEXT&~filters.COMMAND,bozor_dona_h)],
            NARX_SELECT_DATE:[MessageHandler(filters.TEXT&~filters.COMMAND,narx_date)],
            NARX_SHOP_MENU:[MessageHandler(filters.TEXT&~filters.COMMAND,narx_shop_h)],
            NARX_ITEM_MENU:[MessageHandler(filters.TEXT&~filters.COMMAND,narx_item_h)],
            NARX_UNIT_CHANGE:[MessageHandler(filters.TEXT&~filters.COMMAND,narx_unit_ch)],
            NARX_KG_CHANGE:[MessageHandler(filters.TEXT&~filters.COMMAND,narx_kg_ch)],
            NARX_DONA_CHANGE:[MessageHandler(filters.TEXT&~filters.COMMAND,narx_dona_ch)],
            ENTER_PRICE:[MessageHandler(filters.TEXT&~filters.COMMAND,enter_price)],
            HISOBOT_DATE:[MessageHandler(filters.TEXT&~filters.COMMAND,hisobot_date_h)],
            OXIRGI_AMAL_DATE:[MessageHandler(filters.TEXT&~filters.COMMAND,oxirgi_amal_date_h)],
        },
        fallbacks=[CommandHandler("start",start)],
        allow_reentry=True, per_user=True, per_chat=True,
    )
    app.add_handler(conv)
    logger.info("🤖 Bot started!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
