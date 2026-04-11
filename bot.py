#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import asyncio
import threading
import logging
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler
)

# ══════════════════════════════════════════════════════
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN     = os.environ.get("BOT_TOKEN", "YOUR_TOKEN_HERE")
DATA_FILE = os.environ.get("DATA_FILE", "data.json")
PORT      = int(os.environ.get("PORT", 8080))

# ══════════════════════════════════════════════════════
#  HEALTH SERVER  (Render uxlamasin)
# ══════════════════════════════════════════════════════
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(b"OK - Bot ishlayapti!")
    def log_message(self, *a): pass

def health():
    HTTPServer(("0.0.0.0", PORT), H).serve_forever()

# ══════════════════════════════════════════════════════
#  DATA
# ══════════════════════════════════════════════════════
def load():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass
    return {}

def save(data):
    d = os.path.dirname(DATA_FILE)
    if d: os.makedirs(d, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def usr(data, uid):
    uid = str(uid)
    if uid not in data:
        now = datetime.now()
        data[uid] = {
            "year": now.year, "month": now.month,
            "shops": {"Kalbasa do'kon": []},
            "cart": {}, "prices": {}
        }
    return data[uid]

# ══════════════════════════════════════════════════════
#  STATES
# ══════════════════════════════════════════════════════
(S_MAIN, S_CY, S_CM,
 S_LST, S_AS, S_DS, S_SH, S_AI, S_DI,
 S_MD, S_MSH, S_MI, S_MU, S_MQ, S_MAI, S_MDL,
 S_PD, S_PSH, S_PI, S_PCU, S_PKG, S_PDN, S_PE,
 S_RD, S_ST, S_UD, S_BD) = range(27)

MONTHS = ["Yanvar","Fevral","Mart","Aprel","May","Iyun",
          "Iyul","Avgust","Sentabr","Oktabr","Noyabr","Dekabr"]
ME = ["❄️","🌧️","🌱","🌸","🌻","☀️","🏖️","🍉","🍂","🎃","🍁","🎄"]

def kb(r): return ReplyKeyboardMarkup(r, resize_keyboard=True, one_time_keyboard=False)
def MKB(): return kb([
    ["📋 Ro'yxat kiritish",   "🛒 Bozorga ro'yxat"],
    ["💰 Narxlarni kiritish", "📊 Kunlik xisobot"],
    ["📈 Statistika",         "🗑 Oxirgi amalni o'chirish"],
    ["🧺 Savat jami narxi"],
    ["📅 Yilni o'zgartirish", "🗓 Oyni o'zgartirish"],
])
def DKB(): 
    r=[]
    for s in range(0,31,7): r.append([str(d) for d in range(s+1,min(s+8,32))])
    r.append(["🔙 Orqaga"]); return kb(r)

def fmt(n):
    try: return f"{int(float(n)):,}".replace(",", " ")
    except: return str(n)

def dkey(u, day):
    return f"{int(day):02d}.{u['month']:02d}.{u['year']}"

async def tmp(ctx, cid, text, d=2):
    try:
        m = await ctx.bot.send_message(cid, text)
        await asyncio.sleep(d)
        await ctx.bot.delete_message(cid, m.message_id)
    except: pass

def pdates(u): return sorted(u.get("prices", {}).keys())

def last_up(u, shop, item, excl=None):
    best_d = best_p = None
    for d, sh in u.get("prices", {}).items():
        if d == excl: continue
        p = sh.get(shop, {}).get(item, {}).get("unit_price")
        if p is not None and (best_d is None or d > best_d):
            best_d, best_p = d, p
    return best_p

# ══════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    data = load(); u = usr(data, update.effective_user.id); save(data)
    ctx.user_data.clear()
    await update.message.reply_text(
        f"👋 Salom! Xush kelibsiz!\n📅 {MONTHS[u['month']-1]} {u['year']}",
        reply_markup=MKB())
    return S_MAIN

# ══════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════
async def main_h(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    t = update.message.text
    if   t == "📋 Ro'yxat kiritish":          return await lst(update, ctx)
    elif t == "🛒 Bozorga ro'yxat":            return await mdt(update, ctx)
    elif t == "💰 Narxlarni kiritish":         return await pdt(update, ctx)
    elif t == "📊 Kunlik xisobot":             return await rdt(update, ctx)
    elif t == "📈 Statistika":                 return await stat(update, ctx)
    elif t == "🗑 Oxirgi amalni o'chirish":    return await undo(update, ctx)
    elif t == "🧺 Savat jami narxi":           return await bsk(update, ctx)
    elif t == "📅 Yilni o'zgartirish":         return await cy(update, ctx)
    elif t == "🗓 Oyni o'zgartirish":          return await cm(update, ctx)
    return S_MAIN

# ══════════════════════════════════════════════════════
#  YIL / OY
# ══════════════════════════════════════════════════════
async def cy(update, ctx):
    data = load(); u = usr(data, update.effective_user.id); yr = u["year"]
    await update.message.reply_text(f"📅 Hozirgi yil: {yr}",
        reply_markup=kb([[f"◀️ {yr-1}", f"✅ {yr}", f"▶️ {yr+1}"], ["🔙 Orqaga"]]))
    return S_CY

async def cy_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    if t == "🔙 Orqaga":
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    data = load(); u = usr(data, uid); yr = u["year"]
    if f"◀️ {yr-1}" in t: u["year"] = yr-1
    elif f"▶️ {yr+1}" in t: u["year"] = yr+1
    save(data)
    await tmp(ctx, update.effective_chat.id, f"✅ Yil → {u['year']}")
    await update.message.reply_text(f"📅 {MONTHS[u['month']-1]} {u['year']}", reply_markup=MKB())
    return S_MAIN

async def cm(update, ctx):
    r = [[f"{ME[i]} {MONTHS[i]}" for i in range(j,j+3)] for j in range(0,12,3)]
    r.append(["🔙 Orqaga"])
    await update.message.reply_text("🗓 Oyni tanlang:", reply_markup=kb(r)); return S_CM

async def cm_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    if t == "🔙 Orqaga":
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    for i, m in enumerate(MONTHS):
        if m in t:
            data = load(); u = usr(data, uid); u["month"] = i+1; save(data)
            await tmp(ctx, update.effective_chat.id, f"✅ Oy → {m}")
            await update.message.reply_text(f"📅 {m} {u['year']}", reply_markup=MKB())
            return S_MAIN
    r = [[f"{ME[i]} {MONTHS[i]}" for i in range(j,j+3)] for j in range(0,12,3)]
    r.append(["🔙 Orqaga"])
    await update.message.reply_text("🗓 Oyni tanlang:", reply_markup=kb(r)); return S_CM

# ══════════════════════════════════════════════════════
#  RO'YXAT KIRITISH
# ══════════════════════════════════════════════════════
async def lst(update, ctx):
    data = load(); u = usr(data, str(update.effective_user.id))
    shops = list(u["shops"].keys())
    r = [[f"🏪 {s}"] for s in shops]
    r += [["➕ Do'kon qo'shish","🗑 Do'kon o'chirish"],["🔙 Orqaga"]]
    await update.message.reply_text("🏪 Do'konlar:", reply_markup=kb(r)); return S_LST

async def lst_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    data = load(); u = usr(data, uid)
    if t == "🔙 Orqaga":
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    if t == "➕ Do'kon qo'shish":
        await update.message.reply_text("🏪 Yangi do'kon nomini kiriting:"); return S_AS
    if t == "🗑 Do'kon o'chirish":
        shops = list(u["shops"].keys())
        if not shops:
            await tmp(ctx, update.effective_chat.id, "⚠️ Do'konlar yo'q!")
            return await lst(update, ctx)
        r = [[f"❌ {s}"] for s in shops]+[["🔙 Orqaga"]]
        await update.message.reply_text("O'chiriladigan do'konni tanlang:", reply_markup=kb(r)); return S_DS
    shop = t.replace("🏪 ","")
    if shop in u["shops"]:
        ctx.user_data["shop"] = shop; return await shmenu(update, ctx, shop)
    return S_LST

async def as_h(update, ctx):
    t = update.message.text.strip(); uid = str(update.effective_user.id)
    if t == "🔙 Orqaga": return await lst(update, ctx)
    data = load(); u = usr(data, uid)
    if t not in u["shops"]:
        u["shops"][t] = []; save(data)
        await tmp(ctx, update.effective_chat.id, f"✅ '{t}' qo'shildi!")
    else:
        await tmp(ctx, update.effective_chat.id, "⚠️ Allaqachon bor!")
    return await lst(update, ctx)

async def ds_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    if t == "🔙 Orqaga": return await lst(update, ctx)
    shop = t.replace("❌ ","")
    data = load(); u = usr(data, uid)
    if shop in u["shops"]:
        del u["shops"][shop]; save(data)
        await tmp(ctx, update.effective_chat.id, f"✅ '{shop}' o'chirildi!")
    return await lst(update, ctx)

async def shmenu(update, ctx, shop=None):
    if shop is None: shop = ctx.user_data.get("shop","")
    data = load(); u = usr(data, str(update.effective_user.id))
    items = u["shops"].get(shop, [])
    r = [[f"📦 {it}"] for it in items]
    r += [["➕ Tovar qo'shish","🗑 Tovarni o'chirish"],["🔙 Orqaga"]]
    await update.message.reply_text(f"🏪 {shop}\n📦 Tovarlar:", reply_markup=kb(r)); return S_SH

async def sh_h(update, ctx):
    t = update.message.text; shop = ctx.user_data.get("shop","")
    uid = str(update.effective_user.id); data = load(); u = usr(data, uid)
    if t == "🔙 Orqaga": return await lst(update, ctx)
    if t == "➕ Tovar qo'shish":
        await update.message.reply_text(f"📦 '{shop}' uchun tovar nomini kiriting:"); return S_AI
    if t == "🗑 Tovarni o'chirish":
        items = u["shops"].get(shop, [])
        if not items:
            await tmp(ctx, update.effective_chat.id, "⚠️ Tovarlar yo'q!")
            return await shmenu(update, ctx, shop)
        r = [[f"❌ {it}"] for it in items]+[["🔙 Orqaga"]]
        await update.message.reply_text("O'chiriladigan tovarni tanlang:", reply_markup=kb(r)); return S_DI
    return S_SH

async def ai_h(update, ctx):
    t = update.message.text.strip(); uid = str(update.effective_user.id)
    shop = ctx.user_data.get("shop","")
    if t == "🔙 Orqaga": return await shmenu(update, ctx, shop)
    data = load(); u = usr(data, uid)
    if t not in u["shops"].get(shop, []):
        u["shops"].setdefault(shop,[]).append(t); save(data)
        await tmp(ctx, update.effective_chat.id, f"✅ '{t}' qo'shildi!")
    else:
        await tmp(ctx, update.effective_chat.id, "⚠️ Allaqachon bor!")
    return await shmenu(update, ctx, shop)

async def di_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    shop = ctx.user_data.get("shop","")
    if t == "🔙 Orqaga": return await shmenu(update, ctx, shop)
    item = t.replace("❌ ","")
    data = load(); u = usr(data, uid)
    if item in u["shops"].get(shop, []):
        u["shops"][shop].remove(item); save(data)
        await tmp(ctx, update.effective_chat.id, f"✅ '{item}' o'chirildi!")
    return await shmenu(update, ctx, shop)

# ══════════════════════════════════════════════════════
#  BOZORGA RO'YXAT
# ══════════════════════════════════════════════════════
async def mdt(update, ctx):
    await update.message.reply_text("📅 Sanani tanlang (kun):", reply_markup=DKB()); return S_MD

async def md_h(update, ctx):
    t = update.message.text
    if t == "🔙 Orqaga":
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    try:
        day = int(t)
        if 1 <= day <= 31:
            ctx.user_data["md"] = day; return await mshops(update, ctx)
    except: pass
    await update.message.reply_text("Kun raqamini tanlang:", reply_markup=DKB()); return S_MD

async def mshops(update, ctx):
    uid = str(update.effective_user.id); data = load(); u = usr(data, uid)
    day = ctx.user_data["md"]; d = dkey(u, day)
    shops = list(u["shops"].keys())
    cart = u.get("cart", {}).get(d, {})
    summ = ""
    if cart:
        summ = "\n\n🧺 Savat:\n"
        for sh, items in cart.items():
            if items:
                summ += f"  🏪 {sh}:\n"
                for it, inf in items.items():
                    summ += f"    • {it}: {inf['qty']} {inf['unit']}\n"
    r = [[f"🏪 {s}"] for s in shops]
    r += [["🗑 Savatdan o'chirish"],["🔙 Orqaga"]]
    await update.message.reply_text(f"🛒 Bozorga ro'yxat | 📅 {d}{summ}\n\nDo'konni tanlang:", reply_markup=kb(r))
    return S_MSH

async def msh_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    data = load(); u = usr(data, uid)
    day = ctx.user_data.get("md"); d = dkey(u, day)
    if t == "🔙 Orqaga": return await mdt(update, ctx)
    if t == "🗑 Savatdan o'chirish":
        cart = u.get("cart", {}).get(d, {})
        all_i = [(sh, it) for sh, items in cart.items() for it in items]
        if not all_i:
            await tmp(ctx, update.effective_chat.id, "⚠️ Savat bo'sh!")
            return await mshops(update, ctx)
        r = [[f"❌ {sh} | {it}"] for sh, it in all_i]+[["🔙 Orqaga"]]
        await update.message.reply_text("Savatdan o'chiriladigan tovarni tanlang:", reply_markup=kb(r)); return S_MDL
    shop = t.replace("🏪 ","")
    if shop in u["shops"]:
        ctx.user_data["msh"] = shop; return await mitems(update, ctx)
    return S_MSH

async def mitems(update, ctx):
    uid = str(update.effective_user.id); data = load(); u = usr(data, uid)
    day = ctx.user_data["md"]; shop = ctx.user_data["msh"]; d = dkey(u, day)
    items = u["shops"].get(shop, [])
    cart_i = u.get("cart", {}).get(d, {}).get(shop, {})
    r = []
    for it in items:
        mark = "✅ " if it in cart_i else ""
        r.append([f"{mark}{it}"])
    r += [["➕ Tovar qo'shish"],["🔙 Orqaga"]]
    await update.message.reply_text(f"🏪 {shop}\nNima xarid qilmoqchisiz?", reply_markup=kb(r))
    return S_MI

async def mi_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    data = load(); u = usr(data, uid)
    shop = ctx.user_data.get("msh",""); day = ctx.user_data.get("md"); d = dkey(u, day)
    if t == "🔙 Orqaga": return await mshops(update, ctx)
    if t == "➕ Tovar qo'shish":
        await update.message.reply_text(f"📦 {shop} dan nima olmoqchisiz?"); return S_MAI
    item = t.replace("✅ ","").strip()
    ctx.user_data["mi"] = item
    await update.message.reply_text(f"📦 {item}\nQancha?", reply_markup=kb([["⚖️ kg","🔢 dona"],["🔙 Orqaga"]]))
    return S_MU

async def mai_h(update, ctx):
    t = update.message.text.strip()
    if t == "🔙 Orqaga": return await mitems(update, ctx)
    ctx.user_data["mi"] = t
    await update.message.reply_text(f"📦 {t}\nQancha?", reply_markup=kb([["⚖️ kg","🔢 dona"],["🔙 Orqaga"]]))
    return S_MU

async def mu_h(update, ctx):
    t = update.message.text; item = ctx.user_data.get("mi","")
    if t == "🔙 Orqaga": return await mitems(update, ctx)
    if t == "⚖️ kg":
        ctx.user_data["mu"] = "kg"; ctx.user_data["mq"] = 0.5
        await update.message.reply_text(f"⚖️ {item}\nHozir: 0.5 kg",
            reply_markup=kb([["➖0.5","0.5 kg","➕0.5"],["➖10","10 kg","➕10"],["✅ Saqlash","🔙 Orqaga"]]))
        return S_MQ
    if t == "🔢 dona":
        ctx.user_data["mu"] = "dona"; ctx.user_data["mq"] = 1
        await update.message.reply_text(f"🔢 {item}\nHozir: 1 dona",
            reply_markup=kb([["➖1","1 dona","➕1"],["✅ Saqlash","🔙 Orqaga"]]))
        return S_MQ
    return S_MU

async def mq_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    item = ctx.user_data.get("mi",""); shop = ctx.user_data.get("msh","")
    unit = ctx.user_data.get("mu","dona"); qty = ctx.user_data.get("mq", 1)
    day = ctx.user_data.get("md")
    if t == "🔙 Orqaga": return await mitems(update, ctx)
    if unit == "kg":
        if   t == "0.5 kg": qty = 0.5
        elif t == "10 kg":  qty = 10.0
        elif t == "➕0.5":  qty = round(qty+0.5,1)
        elif t == "➖0.5":  qty = max(0.5,round(qty-0.5,1))
        elif t == "➕10":   qty = round(qty+10,1)
        elif t == "➖10":   qty = max(0.5,round(qty-10,1))
    else:
        if   t == "1 dona": qty = 1
        elif t == "➕1":    qty = qty+1
        elif t == "➖1":    qty = max(1,qty-1)
    ctx.user_data["mq"] = qty
    if t == "✅ Saqlash":
        data = load(); u = usr(data, uid); d = dkey(u, day)
        u.setdefault("cart",{}).setdefault(d,{}).setdefault(shop,{})[item] = {"qty":qty,"unit":unit}
        save(data)
        await tmp(ctx, update.effective_chat.id, f"✅ {item} — {qty} {unit} savatga qo'shildi!")
        return await mitems(update, ctx)
    if unit == "kg":
        await update.message.reply_text(f"⚖️ {item}: {qty} kg",
            reply_markup=kb([["➖0.5","0.5 kg","➕0.5"],["➖10","10 kg","➕10"],["✅ Saqlash","🔙 Orqaga"]]))
    else:
        await update.message.reply_text(f"🔢 {item}: {qty} dona",
            reply_markup=kb([["➖1","1 dona","➕1"],["✅ Saqlash","🔙 Orqaga"]]))
    return S_MQ

async def mdl_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    if t == "🔙 Orqaga": return await mshops(update, ctx)
    entry = t.replace("❌ ","")
    if " | " in entry:
        sh, it = entry.split(" | ",1)
        data = load(); u = usr(data, uid); day = ctx.user_data.get("md"); d = dkey(u, day)
        cart = u.get("cart",{})
        if d in cart and sh in cart[d] and it in cart[d][sh]:
            del cart[d][sh][it]; save(data)
            await tmp(ctx, update.effective_chat.id, f"✅ '{it}' savatdan o'chirildi!")
    return await mshops(update, ctx)

# ══════════════════════════════════════════════════════
#  NARXLARNI KIRITISH
# ══════════════════════════════════════════════════════
async def pdt(update, ctx):
    await update.message.reply_text("📅 Narx kiritiladigan sanani tanlang:", reply_markup=DKB()); return S_PD

async def pd_h(update, ctx):
    t = update.message.text
    if t == "🔙 Orqaga":
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    try:
        day = int(t)
        if 1 <= day <= 31:
            ctx.user_data["pd"] = day; return await pshops(update, ctx)
    except: pass
    await update.message.reply_text("Kun raqamini tanlang:", reply_markup=DKB()); return S_PD

async def pshops(update, ctx):
    uid = str(update.effective_user.id); data = load(); u = usr(data, uid)
    day = ctx.user_data["pd"]; d = dkey(u, day)
    cart = u.get("cart",{}).get(d,{})
    if not cart:
        await tmp(ctx, update.effective_chat.id, f"⚠️ {d} da savat bo'sh! Avval bozorga ro'yxat qo'shing.")
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    r = [[f"🏪 {sh}"] for sh in cart]+[["🔙 Orqaga"]]
    await update.message.reply_text(f"💰 Narx kiritish | 📅 {d}\nDo'konni tanlang:", reply_markup=kb(r)); return S_PSH

async def psh_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    data = load(); u = usr(data, uid); day = ctx.user_data.get("pd"); d = dkey(u, day)
    if t == "🔙 Orqaga": return await pdt(update, ctx)
    shop = t.replace("🏪 ","")
    if shop in u.get("cart",{}).get(d,{}):
        ctx.user_data["psh"] = shop; return await pitems(update, ctx)
    return S_PSH

async def pitems(update, ctx):
    uid = str(update.effective_user.id); data = load(); u = usr(data, uid)
    day = ctx.user_data["pd"]; shop = ctx.user_data["psh"]; d = dkey(u, day)
    cart_i = u.get("cart",{}).get(d,{}).get(shop,{})
    price_i = u.get("prices",{}).get(d,{}).get(shop,{})
    r = []
    for it, inf in cart_i.items():
        done = "✅ " if it in price_i else ""
        r.append([f"{done}{it} ({inf['qty']} {inf['unit']})"])
    r.append(["🔙 Orqaga"])
    await update.message.reply_text(f"💰 {shop} | 📅 {d}\nTovarni tanlang:", reply_markup=kb(r)); return S_PI

async def pi_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    data = load(); u = usr(data, uid)
    day = ctx.user_data.get("pd"); shop = ctx.user_data.get("psh"); d = dkey(u, day)
    if t == "🔙 Orqaga": return await pshops(update, ctx)
    raw = t.replace("✅ ","").strip(); item = raw.split(" (")[0].strip()
    cart_i = u.get("cart",{}).get(d,{}).get(shop,{})
    if item in cart_i:
        inf = cart_i[item]
        ctx.user_data["pi"] = item; ctx.user_data["pq"] = inf["qty"]; ctx.user_data["pu"] = inf["unit"]
        lp = last_up(u, shop, item, excl=d)
        lp_t = f"\n⬇️ Oxirgi narx: {fmt(lp)} so'm" if lp else ""
        await update.message.reply_text(
            f"💰 {item} — {inf['qty']} {inf['unit']} oldingiz\n1 {inf['unit']} narxini kiriting (faqat raqam):{lp_t}",
            reply_markup=kb([["💱 kg/dona o'zgartirish"],["🔙 Orqaga"]]))
        return S_PE
    return S_PI

async def pe_h(update, ctx):
    t = update.message.text.strip(); uid = str(update.effective_user.id)
    data = load(); u = usr(data, uid)
    day = ctx.user_data.get("pd"); shop = ctx.user_data.get("psh")
    item = ctx.user_data.get("pi"); qty = ctx.user_data.get("pq",1); unit = ctx.user_data.get("pu","dona")
    d = dkey(u, day)
    if t == "🔙 Orqaga": return await pitems(update, ctx)
    if t == "💱 kg/dona o'zgartirish":
        await update.message.reply_text("Birlikni tanlang:", reply_markup=kb([["⚖️ kg","🔢 dona"],["🔙 Orqaga"]])); return S_PCU
    try:
        price = float(t.replace(" ","").replace(",","."))
        total = price * qty
        u.setdefault("prices",{}).setdefault(d,{}).setdefault(shop,{})[item] = {
            "qty":qty,"unit":unit,"unit_price":price,"total":total}
        save(data)
        await tmp(ctx, update.effective_chat.id, f"✅ {item}: {qty} {unit} × {fmt(price)} = {fmt(total)} so'm saqlandi!")
        return await pitems(update, ctx)
    except:
        lp = last_up(u, shop, item, excl=d)
        lp_t = f"\n⬇️ Oxirgi narx: {fmt(lp)} so'm" if lp else ""
        await update.message.reply_text(
            f"⚠️ Faqat raqam kiriting!\n{item} narxini kiriting:{lp_t}",
            reply_markup=kb([["💱 kg/dona o'zgartirish"],["🔙 Orqaga"]])); return S_PE

async def pcu_h(update, ctx):
    t = update.message.text; item = ctx.user_data.get("pi","")
    qty = ctx.user_data.get("pq",1)
    if t == "🔙 Orqaga": return await pitems(update, ctx)
    if t == "⚖️ kg":
        ctx.user_data["pu"] = "kg"; ctx.user_data["pq"] = float(qty) if qty else 1.0
        await update.message.reply_text(f"⚖️ {item} — kg tanlang:",
            reply_markup=kb([["➖0.5","0.5 kg","➕0.5"],["➖10","10 kg","➕10"],["✅ Tasdiqlash","🔙 Orqaga"]])); return S_PKG
    if t == "🔢 dona":
        ctx.user_data["pu"] = "dona"; ctx.user_data["pq"] = int(qty) if qty else 1
        await update.message.reply_text(f"🔢 {item} — dona tanlang:",
            reply_markup=kb([["➖1","1 dona","➕1"],["✅ Tasdiqlash","🔙 Orqaga"]])); return S_PDN
    return S_PCU

async def pkg_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    item = ctx.user_data.get("pi",""); shop = ctx.user_data.get("psh","")
    qty = ctx.user_data.get("pq",0.5); day = ctx.user_data.get("pd")
    if t == "🔙 Orqaga": return await pitems(update, ctx)
    if   t == "0.5 kg":  qty = 0.5
    elif t == "10 kg":   qty = 10.0
    elif t == "➕0.5":   qty = round(qty+0.5,1)
    elif t == "➖0.5":   qty = max(0.5,round(qty-0.5,1))
    elif t == "➕10":    qty = round(qty+10,1)
    elif t == "➖10":    qty = max(0.5,round(qty-10,1))
    ctx.user_data["pq"] = qty
    if t == "✅ Tasdiqlash":
        data = load(); u = usr(data, uid); d = dkey(u, day)
        c = u.setdefault("cart",{}).setdefault(d,{}).setdefault(shop,{})
        if item in c: c[item]["qty"]=qty; c[item]["unit"]="kg"
        save(data)
        lp = last_up(u,shop,item,d); lp_t = f"\n⬇️ Oxirgi narx: {fmt(lp)} so'm" if lp else ""
        await update.message.reply_text(
            f"💰 {item} — {qty} kg\n1 kg narxini kiriting:{lp_t}",
            reply_markup=kb([["💱 kg/dona o'zgartirish"],["🔙 Orqaga"]])); return S_PE
    await update.message.reply_text(f"⚖️ {item}: {qty} kg",
        reply_markup=kb([["➖0.5","0.5 kg","➕0.5"],["➖10","10 kg","➕10"],["✅ Tasdiqlash","🔙 Orqaga"]])); return S_PKG

async def pdn_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    item = ctx.user_data.get("pi",""); shop = ctx.user_data.get("psh","")
    qty = ctx.user_data.get("pq",1); day = ctx.user_data.get("pd")
    if t == "🔙 Orqaga": return await pitems(update, ctx)
    if   t == "1 dona":  qty = 1
    elif t == "➕1":     qty = qty+1
    elif t == "➖1":     qty = max(1,qty-1)
    ctx.user_data["pq"] = qty
    if t == "✅ Tasdiqlash":
        data = load(); u = usr(data, uid); d = dkey(u, day)
        c = u.setdefault("cart",{}).setdefault(d,{}).setdefault(shop,{})
        if item in c: c[item]["qty"]=qty; c[item]["unit"]="dona"
        save(data)
        lp = last_up(u,shop,item,d); lp_t = f"\n⬇️ Oxirgi narx: {fmt(lp)} so'm" if lp else ""
        await update.message.reply_text(
            f"💰 {item} — {qty} dona\n1 dona narxini kiriting:{lp_t}",
            reply_markup=kb([["💱 kg/dona o'zgartirish"],["🔙 Orqaga"]])); return S_PE
    await update.message.reply_text(f"🔢 {item}: {qty} dona",
        reply_markup=kb([["➖1","1 dona","➕1"],["✅ Tasdiqlash","🔙 Orqaga"]])); return S_PDN

# ══════════════════════════════════════════════════════
#  KUNLIK XISOBOT
# ══════════════════════════════════════════════════════
async def rdt(update, ctx):
    uid = str(update.effective_user.id); data = load(); u = usr(data, uid)
    dates = pdates(u)
    if not dates:
        await tmp(ctx, update.effective_chat.id, "⚠️ Hech qanday narx kiritilmagan!")
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    r = [[d] for d in dates]+[["🔙 Orqaga"]]
    await update.message.reply_text("📊 Xisobot sanasini tanlang:", reply_markup=kb(r)); return S_RD

async def rd_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    if t == "🔙 Orqaga":
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    data = load(); u = usr(data, uid)
    if t in u.get("prices",{}):
        prices = u["prices"][t]; grand = 0
        txt = f"📊 {t} Xisobot\n{'─'*30}\n"
        for shop, items in prices.items():
            txt += f"\n🏪 {shop}:\n"; sh_t = 0
            for it, inf in items.items():
                total = inf.get("total",0); sh_t += total
                txt += f"  • {it}: {fmt(total)} so'm ({inf['qty']} {inf['unit']})\n"
            txt += f"  📌 {shop} jami: {fmt(sh_t)} so'm\n"; grand += sh_t
        txt += f"\n{'─'*30}\n💰 Umumiy jami: {fmt(grand)} so'm"
        dates = pdates(u); r = [[d] for d in dates]+[["🔙 Orqaga"]]
        await update.message.reply_text(txt, reply_markup=kb(r))
    return S_RD

# ══════════════════════════════════════════════════════
#  STATISTIKA
# ══════════════════════════════════════════════════════
async def stat(update, ctx):
    uid = str(update.effective_user.id); data = load(); u = usr(data, uid)
    dates = pdates(u)
    if not dates:
        await tmp(ctx, update.effective_chat.id, "⚠️ Statistika uchun ma'lumot yo'q!")
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    cur = dates[-1]; prevs = dates[:-1][-2:]; prices = u.get("prices",{})
    current = prices.get(cur,{}); risen = []; fallen = []
    txt = f"📈 Statistika | 📅 {cur}\n{'─'*30}\n"
    for shop, items in current.items():
        txt += f"\n🏪 {shop}:\n"
        for it, inf in items.items():
            cp = inf.get("unit_price",0)
            pl = [(pd, prices.get(pd,{}).get(shop,{}).get(it,{}).get("unit_price",0))
                  for pd in prevs if prices.get(pd,{}).get(shop,{}).get(it)]
            if not pl: icon = "🆕"
            elif any(cp > p for _,p in pl): icon = "❌↗️"; risen.append(it)
            elif any(cp < p for _,p in pl): icon = "⭕️↘️"; fallen.append(it)
            else: icon = "✅"
            pt = " / ".join(f"{pd}-{fmt(p)} so'm" for pd,p in pl)
            txt += f"  • {it}: {icon} ({fmt(cp)} so'm)"
            if pt: txt += f" / ({pt})"
            txt += "\n"
    if risen:  txt += f"\n{'─'*30}\n❌↗️ Oshgan: {', '.join(risen)}"
    if fallen: txt += f"\n⭕️↘️ Tushgan: {', '.join(fallen)}"
    await update.message.reply_text(txt, reply_markup=kb([["🔙 Orqaga"]])); return S_ST

async def st_h(update, ctx):
    if update.message.text == "🔙 Orqaga":
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    return S_ST

# ══════════════════════════════════════════════════════
#  OXIRGI AMALNI O'CHIRISH
# ══════════════════════════════════════════════════════
async def undo(update, ctx):
    uid = str(update.effective_user.id); data = load(); u = usr(data, uid)
    dates = pdates(u)
    if not dates:
        await tmp(ctx, update.effective_chat.id, "⚠️ O'chiriladigan narsa yo'q!")
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    r = [[d] for d in dates]+[["🔙 Orqaga"]]
    await update.message.reply_text("🗑 Qaysi sanani tanlaysiz?", reply_markup=kb(r)); return S_UD

async def ud_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    if t == "🔙 Orqaga":
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    data = load(); u = usr(data, uid); prices = u.get("prices",{})
    if t in prices:
        last_sh = last_it = None
        for sh, items in prices[t].items():
            for it in items: last_sh, last_it = sh, it
        if last_it:
            del prices[t][last_sh][last_it]
            if not prices[t][last_sh]: del prices[t][last_sh]
            if not prices[t]: del prices[t]
            save(data)
            await tmp(ctx, update.effective_chat.id, f"✅ '{last_it}' ({last_sh}) o'chirildi!")
        else:
            await tmp(ctx, update.effective_chat.id, "⚠️ Bu sanada tovar topilmadi!")
    return await undo(update, ctx)

# ══════════════════════════════════════════════════════
#  SAVAT JAMI NARXI
# ══════════════════════════════════════════════════════
async def bsk(update, ctx):
    await update.message.reply_text("🧺 Savat jami uchun sanani tanlang:", reply_markup=DKB()); return S_BD

async def bd_h(update, ctx):
    t = update.message.text; uid = str(update.effective_user.id)
    if t == "🔙 Orqaga":
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    try:
        day = int(t)
        if 1 <= day <= 31:
            data = load(); u = usr(data, uid); d = dkey(u, day)
            cart = u.get("cart",{}).get(d,{})
            if not cart:
                await tmp(ctx, update.effective_chat.id, f"⚠️ {d} da savat bo'sh!")
                await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
            pr_d = u.get("prices",{}).get(d,{}); grand = 0
            txt = f"🧺 Savat jami | 📅 {d}\n{'─'*30}\n"
            for shop, items in cart.items():
                txt += f"\n🏪 {shop}:\n"; sh_t = 0
                for it, inf in items.items():
                    pi = pr_d.get(shop,{}).get(it)
                    lp = pi.get("total",0) if pi else None
                    if lp is None:
                        up = last_up(u,shop,it)
                        if up: lp = up*inf["qty"]
                    if lp is not None:
                        txt += f"  • {it}: ~{fmt(lp)} so'm ({inf['qty']} {inf['unit']})\n"; sh_t += lp
                    else:
                        txt += f"  • {it}: narx yo'q ({inf['qty']} {inf['unit']})\n"
                txt += f"  📌 ~{shop} jami: {fmt(sh_t)} so'm\n"; grand += sh_t
            txt += f"\n{'─'*30}\n💰 Umumiy ~jami: {fmt(grand)} so'm"
            await update.message.reply_text(txt, reply_markup=kb([["🔙 Orqaga"]])); return S_BD
    except: pass
    await update.message.reply_text("Kun raqamini tanlang:", reply_markup=DKB()); return S_BD

async def bd_back(update, ctx):
    if update.message.text == "🔙 Orqaga":
        await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN
    return S_BD

# ══════════════════════════════════════════════════════
#  FALLBACK
# ══════════════════════════════════════════════════════
async def fallback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🏠 Asosiy menyu", reply_markup=MKB()); return S_MAIN

# ══════════════════════════════════════════════════════
#  RUN
# ══════════════════════════════════════════════════════
def main():
    threading.Thread(target=health, daemon=True).start()
    logger.info(f"✅ Health server :{PORT}")

    app = Application.builder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            S_MAIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, main_h)],
            S_CY:   [MessageHandler(filters.TEXT & ~filters.COMMAND, cy_h)],
            S_CM:   [MessageHandler(filters.TEXT & ~filters.COMMAND, cm_h)],
            S_LST:  [MessageHandler(filters.TEXT & ~filters.COMMAND, lst_h)],
            S_AS:   [MessageHandler(filters.TEXT & ~filters.COMMAND, as_h)],
            S_DS:   [MessageHandler(filters.TEXT & ~filters.COMMAND, ds_h)],
            S_SH:   [MessageHandler(filters.TEXT & ~filters.COMMAND, sh_h)],
            S_AI:   [MessageHandler(filters.TEXT & ~filters.COMMAND, ai_h)],
            S_DI:   [MessageHandler(filters.TEXT & ~filters.COMMAND, di_h)],
            S_MD:   [MessageHandler(filters.TEXT & ~filters.COMMAND, md_h)],
            S_MSH:  [MessageHandler(filters.TEXT & ~filters.COMMAND, msh_h)],
            S_MI:   [MessageHandler(filters.TEXT & ~filters.COMMAND, mi_h)],
            S_MU:   [MessageHandler(filters.TEXT & ~filters.COMMAND, mu_h)],
            S_MQ:   [MessageHandler(filters.TEXT & ~filters.COMMAND, mq_h)],
            S_MAI:  [MessageHandler(filters.TEXT & ~filters.COMMAND, mai_h)],
            S_MDL:  [MessageHandler(filters.TEXT & ~filters.COMMAND, mdl_h)],
            S_PD:   [MessageHandler(filters.TEXT & ~filters.COMMAND, pd_h)],
            S_PSH:  [MessageHandler(filters.TEXT & ~filters.COMMAND, psh_h)],
            S_PI:   [MessageHandler(filters.TEXT & ~filters.COMMAND, pi_h)],
            S_PCU:  [MessageHandler(filters.TEXT & ~filters.COMMAND, pcu_h)],
            S_PKG:  [MessageHandler(filters.TEXT & ~filters.COMMAND, pkg_h)],
            S_PDN:  [MessageHandler(filters.TEXT & ~filters.COMMAND, pdn_h)],
            S_PE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, pe_h)],
            S_RD:   [MessageHandler(filters.TEXT & ~filters.COMMAND, rd_h)],
            S_ST:   [MessageHandler(filters.TEXT & ~filters.COMMAND, st_h)],
            S_UD:   [MessageHandler(filters.TEXT & ~filters.COMMAND, ud_h)],
            S_BD:   [MessageHandler(filters.TEXT & ~filters.COMMAND, bd_h)],
        },
        fallbacks=[
            CommandHandler("start", start),
            MessageHandler(filters.TEXT & ~filters.COMMAND, fallback),
        ],
        allow_reentry=True,
        conversation_timeout=600,
    )
    app.add_handler(conv)
    logger.info("🤖 Bot ishga tushdi!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
