import asyncio
import json
import os
from datetime import datetime
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler
)

# ======================== STATES ========================
(
    STATE_YEAR_MONTH,
    STATE_MAIN_MENU,
    STATE_ROYXAT_MENU,
    STATE_ROYXAT_SHOP,
    STATE_ROYXAT_ADD_SHOP,
    STATE_ROYXAT_DEL_SHOP,
    STATE_ROYXAT_TOVAR_ADD,
    STATE_ROYXAT_TOVAR_DEL,
    STATE_BOZOR_DATE,
    STATE_BOZOR_SHOP,
    STATE_BOZOR_TOVAR_SELECT,
    STATE_BOZOR_TOVAR_UNIT,
    STATE_BOZOR_TOVAR_KG,
    STATE_BOZOR_TOVAR_DONA,
    STATE_BOZOR_ADD_NAME,
    STATE_BOZOR_DEL_TOVAR,
    STATE_SAVAT_NARX_DATE,   # yangi: savat jami narxi uchun sana
    STATE_NARX_DATE,
    STATE_NARX_SHOP,
    STATE_NARX_TOVAR,
    STATE_NARX_UNIT_CHANGE,
    STATE_NARX_KG_CHANGE,
    STATE_NARX_DONA_CHANGE,
    STATE_NARX_INPUT,
    STATE_HISOBOT,
    STATE_STATISTIKA,
    STATE_UNDO_DATE,
    STATE_CHANGE_YEAR,
    STATE_CHANGE_MONTH,
) = range(29)

# ======================== DATA FILE ========================
DATA_FILE = "bozor_data.json"

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_user(data, uid):
    uid = str(uid)
    if uid not in data:
        data[uid] = {
            "year": datetime.now().year,
            "month": datetime.now().month,
            "shops": {"🥩 Kalbasa do'kon": []},
            "savat": {},
            "narxlar": {},
        }
    return data[uid]

# ======================== KEYBOARDS ========================
MONTHS_UZ = ["Yanvar","Fevral","Mart","Aprel","May","Iyun",
              "Iyul","Avgust","Sentabr","Oktabr","Noyabr","Dekabr"]

def kb(buttons, resize=True):
    return ReplyKeyboardMarkup(buttons, resize_keyboard=resize)

def year_month_keyboard(year, month):
    return kb([
        [f"◀️ {year-1}", f"📅 {year}", f"▶️ {year+1}"],
        [MONTHS_UZ[i] for i in range(0,4)],
        [MONTHS_UZ[i] for i in range(4,8)],
        [MONTHS_UZ[i] for i in range(8,12)],
    ])

def main_menu_keyboard(year, month):
    month_name = MONTHS_UZ[month-1]
    return kb([
        ["🛒 1. Ro'yxat kiritish", "🏪 2. Bozorga"],
        ["💰 3. Narxlarni kiritish", "📊 4. Kunlik hisobot"],
        ["📈 5. Statistika", "↩️ Oxirgi amalni o'chirish"],
        ["🧮 Savat jami narxi"],   # ← YANGI TUGMA
        [f"◀️ {year-1}", f"📅 {year} / {month_name}", f"▶️ {year+1}"],
        [MONTHS_UZ[i] for i in range(0,6)],
        [MONTHS_UZ[i] for i in range(6,12)],
    ])

def back_keyboard():
    return kb([["🔙 Ortga"]])

def dates_keyboard(dates, extra_back=True):
    rows = []
    row = []
    for d in dates:
        row.append(d)
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    if extra_back:
        rows.append(["🔙 Ortga"])
    return kb(rows)

def numbers_keyboard(n=31):
    rows = []
    row = []
    for i in range(1, n+1):
        row.append(str(i))
        if len(row) == 7:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(["🔙 Ortga"])
    return kb(rows)

def shops_keyboard(shops, extra=True):
    rows = [[s] for s in shops]
    if extra:
        rows.append(["➕ Do'kon qo'shish", "🗑 Do'kon o'chirish"])
    rows.append(["🔙 Ortga"])
    return kb(rows)

def tovar_keyboard(tovars, show_add=True):
    rows = [[t] for t in tovars]
    if show_add:
        rows.append(["➕ Tovar qo'shish", "🗑 Tovarni o'chirish"])
    rows.append(["🔙 Ortga"])
    return kb(rows)

def bozor_shop_keyboard(shops):
    rows = [[s] for s in shops]
    rows.append(["🔙 Ortga"])
    return kb(rows)

def bozor_tovar_keyboard(shop_tovars, savat_tovars):
    rows = []
    for t in shop_tovars:
        name = t if isinstance(t, str) else t
        in_savat = any(s["name"] == name for s in savat_tovars)
        label = f"✅ {name}" if in_savat else name
        rows.append([label])
    rows.append(["➕ Tovar qo'shish", "🗑 O'chirish (savatdan)"])
    rows.append(["🔙 Ortga"])
    return kb(rows)

def unit_keyboard():
    return kb([["⚖️ kg", "🔢 Dona"], ["🔙 Ortga"]])

def kg_keyboard(current):
    return kb([
        [f"➖0.5", f"⚖️ {current}kg", f"➕0.5"],
        [f"➖10", f"⚖️ {current}kg (10)", f"➕10"],
        ["✅ Saqlash", "🔙 Ortga"]
    ])

def dona_keyboard(current):
    return kb([
        [f"➖1", f"🔢 {current} dona", f"➕1"],
        ["✅ Saqlash", "🔙 Ortga"]
    ])

# ======================== HELPERS ========================
def get_savat_key(year, month, day):
    return f"{year}-{month:02d}-{day:02d}"

def format_date(year, month, day):
    return f"{day:02d}.{month:02d}.{year}"

def parse_date_key(key):
    parts = key.split("-")
    return int(parts[0]), int(parts[1]), int(parts[2])

async def send_temp(update, text, delay=1.5):
    msg = await update.message.reply_text(text)
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except:
        pass

# ======================== SAVAT JAMI NARXI (YANGI) ========================
def calculate_savat_total(u, savat_key):
    """
    Savatdagi tovarlarning narxlarini narxlar bazasidan olib,
    umumiy summani hisoblaydi.
    Agar tovar narxi kiritilmagan bo'lsa, oxirgi kiritilgan narxdan foydalanadi.
    """
    savat_data = u.get("savat", {}).get(savat_key, {})
    narxlar = u.get("narxlar", {})

    # Barcha narx kalitlarini saralangan holda olish
    all_narx_keys = sorted(narxlar.keys())

    results = []
    grand_total = 0
    has_missing = False

    for shop, items in savat_data.items():
        shop_total = 0
        shop_lines = []
        for item in items:
            name = item["name"]
            qty = item["qty"]
            unit = item.get("unit", "kg")
            unit_label = "kg" if unit == "kg" else "dona"

            # 1. Aynan shu sanada narx kiritilganmi?
            cur_narx = narxlar.get(savat_key, {}).get(shop, {}).get(name)
            if cur_narx:
                price_per = cur_narx.get("price_per_unit", 0)
                total = price_per * qty
                shop_lines.append(
                    f"  ✅ {name}: {qty}{unit_label} × {price_per:,.0f} = {total:,.0f} so'm"
                )
                shop_total += total
            else:
                # 2. Oxirgi kiritilgan narxni qidirish (eng yaqin sana)
                last_price = None
                last_date = None
                for k in reversed(all_narx_keys):
                    if k == savat_key:
                        continue
                    p = narxlar[k].get(shop, {}).get(name, {})
                    if p and p.get("price_per_unit"):
                        last_price = p["price_per_unit"]
                        last_date = format_date(*parse_date_key(k))
                        break

                if last_price:
                    total = last_price * qty
                    shop_lines.append(
                        f"  ⚠️ {name}: {qty}{unit_label} × {last_price:,.0f}* = {total:,.0f} so'm\n"
                        f"     (*{last_date} dagi narx)"
                    )
                    shop_total += total
                    has_missing = True
                else:
                    shop_lines.append(
                        f"  ❓ {name}: {qty}{unit_label} — narx yo'q"
                    )
                    has_missing = True

        results.append((shop, shop_lines, shop_total))
        grand_total += shop_total

    return results, grand_total, has_missing


async def savat_narx_date_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Savat jami narxi uchun sana tanlash."""
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        await update.message.reply_text(
            "🏠 Asosiy menyu:",
            reply_markup=main_menu_keyboard(u["year"], u["month"])
        )
        return STATE_MAIN_MENU

    try:
        day = int(text)
        if 1 <= day <= 31:
            savat_key = get_savat_key(u["year"], u["month"], day)
            savat_data = u.get("savat", {}).get(savat_key, {})

            if not savat_data:
                await send_temp(
                    update,
                    f"❌ {format_date(u['year'], u['month'], day)} uchun savat bo'sh."
                )
                await update.message.reply_text(
                    "📅 Sanani tanlang:",
                    reply_markup=numbers_keyboard(31)
                )
                return STATE_SAVAT_NARX_DATE

            # Hisoblash
            results, grand_total, has_missing = calculate_savat_total(u, savat_key)

            date_str = format_date(u["year"], u["month"], day)
            msg = f"🧮 {date_str} — Savat Jami Narxi\n{'═'*32}\n"

            for shop, lines, shop_total in results:
                msg += f"\n🏪 {shop}:\n"
                msg += "\n".join(lines)
                msg += f"\n  💰 Do'kon jami: {shop_total:,.0f} so'm\n"

            msg += f"\n{'═'*32}\n"
            msg += f"💵 UMUMIY JAMI: {grand_total:,.0f} so'm\n"

            if has_missing:
                msg += "\n⚠️ = oxirgi kiritilgan narx ishlatildi\n❓ = narx hali kiritilmagan"

            # Pulim yetadimi?
            msg += f"\n\n{'─'*32}"
            msg += f"\n💡 Sizda qancha pul bor ekan?\n(Yuqoridagi summani hisobga oling)"

            await update.message.reply_text(
                msg,
                reply_markup=main_menu_keyboard(u["year"], u["month"])
            )
            return STATE_MAIN_MENU

    except Exception as e:
        pass

    await update.message.reply_text("📅 Sanani tanlang:", reply_markup=numbers_keyboard(31))
    return STATE_SAVAT_NARX_DATE


# ======================== START ========================
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    save_data(data)
    year = u["year"]
    month = u["month"]
    await update.message.reply_text(
        f"👋 Xush kelibsiz!\n📅 Yil va oyni tanlang:\n\nHozirgi: {year} / {MONTHS_UZ[month-1]}",
        reply_markup=year_month_keyboard(year, month)
    )
    return STATE_YEAR_MONTH

async def year_month_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text.startswith("◀️"):
        u["year"] -= 1
        save_data(data)
        await update.message.reply_text(
            f"📅 {u['year']} / {MONTHS_UZ[u['month']-1]}",
            reply_markup=year_month_keyboard(u["year"], u["month"])
        )
        return STATE_YEAR_MONTH
    elif text.startswith("▶️"):
        u["year"] += 1
        save_data(data)
        await update.message.reply_text(
            f"📅 {u['year']} / {MONTHS_UZ[u['month']-1]}",
            reply_markup=year_month_keyboard(u["year"], u["month"])
        )
        return STATE_YEAR_MONTH
    elif text.startswith("📅"):
        pass
    else:
        for i, m in enumerate(MONTHS_UZ):
            if text == m:
                u["month"] = i + 1
                save_data(data)
                await update.message.reply_text(
                    f"✅ {u['year']} yil, {MONTHS_UZ[u['month']-1]} tanlandi!\n\n🏠 Asosiy menyu:",
                    reply_markup=main_menu_keyboard(u["year"], u["month"])
                )
                return STATE_MAIN_MENU

    await update.message.reply_text(
        f"📅 {u['year']} / {MONTHS_UZ[u['month']-1]} — Oy tanlang:",
        reply_markup=year_month_keyboard(u["year"], u["month"])
    )
    return STATE_YEAR_MONTH

# ======================== MAIN MENU ========================
async def main_menu_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text.startswith("◀️"):
        u["year"] -= 1
        save_data(data)
        await update.message.reply_text(
            f"📅 Yil o'zgartirildi: {u['year']}",
            reply_markup=main_menu_keyboard(u["year"], u["month"])
        )
        return STATE_MAIN_MENU
    elif text.startswith("▶️"):
        u["year"] += 1
        save_data(data)
        await update.message.reply_text(
            f"📅 Yil o'zgartirildi: {u['year']}",
            reply_markup=main_menu_keyboard(u["year"], u["month"])
        )
        return STATE_MAIN_MENU
    elif text.startswith("📅"):
        return STATE_MAIN_MENU

    for i, m in enumerate(MONTHS_UZ):
        if text == m:
            u["month"] = i + 1
            save_data(data)
            await update.message.reply_text(
                f"✅ Oy o'zgartirildi: {MONTHS_UZ[u['month']-1]}",
                reply_markup=main_menu_keyboard(u["year"], u["month"])
            )
            return STATE_MAIN_MENU

    if "Ro'yxat kiritish" in text:
        shops = list(u["shops"].keys())
        await update.message.reply_text(
            "🏪 Do'konlar ro'yxati:\nDo'konni tanlang yoki yangi qo'shing.",
            reply_markup=shops_keyboard(shops)
        )
        return STATE_ROYXAT_MENU

    elif "Bozorga" in text:
        await update.message.reply_text(
            "📅 Sanani tanlang (kun):",
            reply_markup=numbers_keyboard(31)
        )
        ctx.user_data["bozor_step"] = "date"
        return STATE_BOZOR_DATE

    elif "Narxlarni kiritish" in text:
        await update.message.reply_text(
            "📅 Sanani tanlang:",
            reply_markup=numbers_keyboard(31)
        )
        return STATE_NARX_DATE

    elif "Kunlik hisobot" in text:
        narxlar = u.get("narxlar", {})
        dates = [k for k in narxlar.keys() if k.startswith(f"{u['year']}-{u['month']:02d}-")]
        if not dates:
            await update.message.reply_text(
                "❌ Narx kiritilgan sana topilmadi.",
                reply_markup=main_menu_keyboard(u["year"], u["month"])
            )
            return STATE_MAIN_MENU
        display = [format_date(*parse_date_key(d)) for d in sorted(dates)]
        await update.message.reply_text(
            "📊 Hisobot uchun sanani tanlang:",
            reply_markup=dates_keyboard(display)
        )
        return STATE_HISOBOT

    elif "Statistika" in text:
        await statistika_show(update, u)
        return STATE_MAIN_MENU

    elif "Oxirgi amalni o'chirish" in text:
        narxlar = u.get("narxlar", {})
        dates = [k for k in narxlar.keys() if k.startswith(f"{u['year']}-{u['month']:02d}-")]
        if not dates:
            await update.message.reply_text(
                "❌ O'chirish uchun ma'lumot yo'q.",
                reply_markup=main_menu_keyboard(u["year"], u["month"])
            )
            return STATE_MAIN_MENU
        display = [format_date(*parse_date_key(d)) for d in sorted(dates)]
        await update.message.reply_text(
            "🗑 Qaysi sanadagi oxirgi amalni o'chirmoqchisiz?",
            reply_markup=dates_keyboard(display)
        )
        return STATE_UNDO_DATE

    # ── YANGI: Savat jami narxi ──────────────────────────────────────────────
    elif "Savat jami narxi" in text:
        # Savatda ma'lumot bor sanalarni topish
        savat = u.get("savat", {})
        month_prefix = f"{u['year']}-{u['month']:02d}-"
        savat_dates = sorted([k for k in savat.keys() if k.startswith(month_prefix) and savat[k]])

        if not savat_dates:
            await update.message.reply_text(
                "❌ Savatda hech qanday tovar yo'q.\n"
                "Avval 🏪 Bozorga bo'limida tovar qo'shing.",
                reply_markup=main_menu_keyboard(u["year"], u["month"])
            )
            return STATE_MAIN_MENU

        # Sanalarni ko'rsatish
        display = [format_date(*parse_date_key(d)) for d in savat_dates]
        await update.message.reply_text(
            "🧮 *Savat jami narxi*\n\n"
            "Qaysi kun uchun hisob chiqarilsin?\n"
            "_(Savatdagi tovarlarning umumiy narxi ko'rsatiladi)_",
            parse_mode="Markdown",
            reply_markup=dates_keyboard(display)
        )
        return STATE_SAVAT_NARX_DATE

    return STATE_MAIN_MENU

# ======================== RO'YXAT ========================
async def royxat_menu_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        await update.message.reply_text("🏠 Asosiy menyu:", reply_markup=main_menu_keyboard(u["year"], u["month"]))
        return STATE_MAIN_MENU

    if text == "➕ Do'kon qo'shish":
        await update.message.reply_text("🏪 Yangi do'kon nomini kiriting:", reply_markup=back_keyboard())
        return STATE_ROYXAT_ADD_SHOP

    if text == "🗑 Do'kon o'chirish":
        shops = [s for s in u["shops"].keys()]
        if not shops:
            await send_temp(update, "❌ Do'kon yo'q.")
            return STATE_ROYXAT_MENU
        rows = [[s] for s in shops]
        rows.append(["🔙 Ortga"])
        await update.message.reply_text("🗑 Qaysi do'konni o'chirmoqchisiz?", reply_markup=kb(rows))
        return STATE_ROYXAT_DEL_SHOP

    shops = list(u["shops"].keys())
    if text in shops:
        ctx.user_data["current_shop"] = text
        tovars = u["shops"][text]
        await update.message.reply_text(
            f"🏪 {text}\n📦 Tovarlar:",
            reply_markup=tovar_keyboard(tovars)
        )
        return STATE_ROYXAT_SHOP

    await update.message.reply_text("Do'konni tanlang:", reply_markup=shops_keyboard(shops))
    return STATE_ROYXAT_MENU

async def royxat_add_shop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        await update.message.reply_text("🏪 Do'konlar:", reply_markup=shops_keyboard(list(u["shops"].keys())))
        return STATE_ROYXAT_MENU

    if text in u["shops"]:
        await send_temp(update, "⚠️ Bu do'kon allaqachon mavjud!")
        await update.message.reply_text("🏪 Do'konlar:", reply_markup=shops_keyboard(list(u["shops"].keys())))
        return STATE_ROYXAT_MENU

    u["shops"][text] = []
    save_data(data)
    await send_temp(update, f"✅ '{text}' do'koni qo'shildi!")
    await update.message.reply_text("🏪 Do'konlar:", reply_markup=shops_keyboard(list(u["shops"].keys())))
    return STATE_ROYXAT_MENU

async def royxat_del_shop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        await update.message.reply_text("🏪 Do'konlar:", reply_markup=shops_keyboard(list(u["shops"].keys())))
        return STATE_ROYXAT_MENU

    if text in u["shops"]:
        del u["shops"][text]
        save_data(data)
        await send_temp(update, f"✅ '{text}' o'chirildi!")

    await update.message.reply_text("🏪 Do'konlar:", reply_markup=shops_keyboard(list(u["shops"].keys())))
    return STATE_ROYXAT_MENU

async def royxat_shop_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    shop = ctx.user_data.get("current_shop")

    if text == "🔙 Ortga":
        await update.message.reply_text("🏪 Do'konlar:", reply_markup=shops_keyboard(list(u["shops"].keys())))
        return STATE_ROYXAT_MENU

    if text == "➕ Tovar qo'shish":
        await update.message.reply_text(f"📦 {shop} — Tovar nomini kiriting:", reply_markup=back_keyboard())
        return STATE_ROYXAT_TOVAR_ADD

    if text == "🗑 Tovarni o'chirish":
        tovars = u["shops"].get(shop, [])
        if not tovars:
            await send_temp(update, "❌ Tovar yo'q.")
            return STATE_ROYXAT_SHOP
        rows = [[t] for t in tovars]
        rows.append(["🔙 Ortga"])
        await update.message.reply_text("🗑 Qaysi tovarni o'chirmoqchisiz?", reply_markup=kb(rows))
        return STATE_ROYXAT_TOVAR_DEL

    return STATE_ROYXAT_SHOP

async def royxat_tovar_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    shop = ctx.user_data.get("current_shop")

    if text == "🔙 Ortga":
        await update.message.reply_text(f"🏪 {shop}:", reply_markup=tovar_keyboard(u["shops"].get(shop, [])))
        return STATE_ROYXAT_SHOP

    if shop and text not in u["shops"].get(shop, []):
        u["shops"][shop].append(text)
        save_data(data)
        await send_temp(update, f"✅ '{text}' qo'shildi!")
    else:
        await send_temp(update, "⚠️ Bu tovar allaqachon mavjud!")

    await update.message.reply_text(f"🏪 {shop}:", reply_markup=tovar_keyboard(u["shops"].get(shop, [])))
    return STATE_ROYXAT_SHOP

async def royxat_tovar_del(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    shop = ctx.user_data.get("current_shop")

    if text == "🔙 Ortga":
        await update.message.reply_text(f"🏪 {shop}:", reply_markup=tovar_keyboard(u["shops"].get(shop, [])))
        return STATE_ROYXAT_SHOP

    if shop and text in u["shops"].get(shop, []):
        u["shops"][shop].remove(text)
        save_data(data)
        await send_temp(update, f"✅ '{text}' o'chirildi!")

    await update.message.reply_text(f"🏪 {shop}:", reply_markup=tovar_keyboard(u["shops"].get(shop, [])))
    return STATE_ROYXAT_SHOP

# ======================== BOZORGA ========================
async def bozor_date_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        await update.message.reply_text("🏠 Asosiy menyu:", reply_markup=main_menu_keyboard(u["year"], u["month"]))
        return STATE_MAIN_MENU

    try:
        day = int(text)
        if 1 <= day <= 31:
            ctx.user_data["bozor_day"] = day
            savat_key = get_savat_key(u["year"], u["month"], day)
            ctx.user_data["savat_key"] = savat_key
            shops = list(u["shops"].keys())
            await update.message.reply_text(
                f"📅 {format_date(u['year'], u['month'], day)} — Do'konni tanlang:",
                reply_markup=bozor_shop_keyboard(shops)
            )
            return STATE_BOZOR_SHOP
    except:
        pass

    await update.message.reply_text("📅 Kun raqamini tanlang:", reply_markup=numbers_keyboard(31))
    return STATE_BOZOR_DATE

async def bozor_shop_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        await update.message.reply_text("📅 Sanani tanlang:", reply_markup=numbers_keyboard(31))
        return STATE_BOZOR_DATE

    shops = list(u["shops"].keys())
    if text in shops:
        ctx.user_data["bozor_shop"] = text
        savat_key = ctx.user_data.get("savat_key")
        savat = u.get("savat", {}).get(savat_key, {}).get(text, [])
        shop_tovars = u["shops"][text]
        await update.message.reply_text(
            f"🛒 {text} — Tovarlarni tanlang:\n(✅ = savatda)",
            reply_markup=bozor_tovar_keyboard(shop_tovars, savat)
        )
        return STATE_BOZOR_TOVAR_SELECT

    await update.message.reply_text("Do'konni tanlang:", reply_markup=bozor_shop_keyboard(shops))
    return STATE_BOZOR_SHOP

async def bozor_tovar_select_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    shop = ctx.user_data.get("bozor_shop")
    savat_key = ctx.user_data.get("savat_key")

    if text == "🔙 Ortga":
        shops = list(u["shops"].keys())
        await update.message.reply_text("Do'konni tanlang:", reply_markup=bozor_shop_keyboard(shops))
        return STATE_BOZOR_SHOP

    if text == "➕ Tovar qo'shish":
        await update.message.reply_text(
            f"🛒 {shop} — Bu do'kondan nima olmoqchisiz? (nom kiriting):",
            reply_markup=back_keyboard()
        )
        return STATE_BOZOR_ADD_NAME

    if text == "🗑 O'chirish (savatdan)":
        savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
        if not savat:
            await send_temp(update, "❌ Savatda tovar yo'q.")
            return STATE_BOZOR_TOVAR_SELECT
        rows = [[item["name"]] for item in savat]
        rows.append(["🔙 Ortga"])
        await update.message.reply_text("🗑 Qaysi tovarni savatdan o'chirmoqchisiz?", reply_markup=kb(rows))
        return STATE_BOZOR_DEL_TOVAR

    clean_name = text.replace("✅ ", "").strip()
    shop_tovars = u["shops"].get(shop, [])
    if clean_name in shop_tovars:
        ctx.user_data["bozor_tovar"] = clean_name
        savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
        existing = next((s for s in savat if s["name"] == clean_name), None)
        if existing:
            unit = existing.get("unit", "kg")
            qty = existing.get("qty", 1.0)
            if unit == "kg":
                await update.message.reply_text(
                    f"⚖️ {clean_name} — Miqdorni tanlang:",
                    reply_markup=kg_keyboard(qty)
                )
                ctx.user_data["bozor_qty"] = qty
                ctx.user_data["bozor_unit"] = "kg"
                return STATE_BOZOR_TOVAR_KG
            else:
                await update.message.reply_text(
                    f"🔢 {clean_name} — Donasini tanlang:",
                    reply_markup=dona_keyboard(int(qty))
                )
                ctx.user_data["bozor_qty"] = qty
                ctx.user_data["bozor_unit"] = "dona"
                return STATE_BOZOR_TOVAR_DONA
        else:
            await update.message.reply_text(
                f"📦 {clean_name} — Birlikni tanlang:",
                reply_markup=unit_keyboard()
            )
            return STATE_BOZOR_TOVAR_UNIT

    savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
    await update.message.reply_text("Tovarni tanlang:", reply_markup=bozor_tovar_keyboard(shop_tovars, savat))
    return STATE_BOZOR_TOVAR_SELECT

async def bozor_tovar_unit_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        shop = ctx.user_data.get("bozor_shop")
        savat_key = ctx.user_data.get("savat_key")
        savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
        await update.message.reply_text("Tovarni tanlang:", reply_markup=bozor_tovar_keyboard(u["shops"].get(shop, []), savat))
        return STATE_BOZOR_TOVAR_SELECT

    if "kg" in text.lower():
        ctx.user_data["bozor_unit"] = "kg"
        ctx.user_data["bozor_qty"] = 1.0
        await update.message.reply_text(f"⚖️ Miqdorni tanlang (kg):", reply_markup=kg_keyboard(1.0))
        return STATE_BOZOR_TOVAR_KG

    if "dona" in text.lower():
        ctx.user_data["bozor_unit"] = "dona"
        ctx.user_data["bozor_qty"] = 1
        await update.message.reply_text(f"🔢 Donasini tanlang:", reply_markup=dona_keyboard(1))
        return STATE_BOZOR_TOVAR_DONA

    await update.message.reply_text("Birlikni tanlang:", reply_markup=unit_keyboard())
    return STATE_BOZOR_TOVAR_UNIT

async def bozor_tovar_kg_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    qty = ctx.user_data.get("bozor_qty", 1.0)

    if text == "🔙 Ortga":
        await update.message.reply_text("Birlikni tanlang:", reply_markup=unit_keyboard())
        return STATE_BOZOR_TOVAR_UNIT

    if "➖0.5" in text:
        qty = max(0.5, round(qty - 0.5, 1))
    elif "➕0.5" in text:
        qty = round(qty + 0.5, 1)
    elif "➖10" in text:
        qty = max(0.5, round(qty - 10, 1))
    elif "➕10" in text:
        qty = round(qty + 10, 1)
    elif "Saqlash" in text:
        shop = ctx.user_data.get("bozor_shop")
        savat_key = ctx.user_data.get("savat_key")
        tovar = ctx.user_data.get("bozor_tovar")
        if "savat" not in u:
            u["savat"] = {}
        if savat_key not in u["savat"]:
            u["savat"][savat_key] = {}
        if shop not in u["savat"][savat_key]:
            u["savat"][savat_key][shop] = []
        savat = u["savat"][savat_key][shop]
        existing = next((s for s in savat if s["name"] == tovar), None)
        if existing:
            existing["qty"] = qty
            existing["unit"] = "kg"
        else:
            savat.append({"name": tovar, "qty": qty, "unit": "kg"})
        save_data(data)
        await send_temp(update, f"✅ {tovar} — {qty}kg savatga qo'shildi!")
        savat_updated = u["savat"][savat_key][shop]
        await update.message.reply_text(
            f"🛒 {shop} — Tovarlarni tanlang:",
            reply_markup=bozor_tovar_keyboard(u["shops"].get(shop, []), savat_updated)
        )
        return STATE_BOZOR_TOVAR_SELECT

    ctx.user_data["bozor_qty"] = qty
    await update.message.reply_text(f"⚖️ Miqdor: {qty}kg", reply_markup=kg_keyboard(qty))
    return STATE_BOZOR_TOVAR_KG

async def bozor_tovar_dona_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    qty = int(ctx.user_data.get("bozor_qty", 1))

    if text == "🔙 Ortga":
        await update.message.reply_text("Birlikni tanlang:", reply_markup=unit_keyboard())
        return STATE_BOZOR_TOVAR_UNIT

    if "➖1" in text:
        qty = max(1, qty - 1)
    elif "➕1" in text:
        qty = qty + 1
    elif "Saqlash" in text:
        shop = ctx.user_data.get("bozor_shop")
        savat_key = ctx.user_data.get("savat_key")
        tovar = ctx.user_data.get("bozor_tovar")
        if "savat" not in u:
            u["savat"] = {}
        if savat_key not in u["savat"]:
            u["savat"][savat_key] = {}
        if shop not in u["savat"][savat_key]:
            u["savat"][savat_key][shop] = []
        savat = u["savat"][savat_key][shop]
        existing = next((s for s in savat if s["name"] == tovar), None)
        if existing:
            existing["qty"] = qty
            existing["unit"] = "dona"
        else:
            savat.append({"name": tovar, "qty": qty, "unit": "dona"})
        save_data(data)
        await send_temp(update, f"✅ {tovar} — {qty} dona savatga qo'shildi!")
        savat_updated = u["savat"][savat_key][shop]
        await update.message.reply_text(
            f"🛒 {shop}:",
            reply_markup=bozor_tovar_keyboard(u["shops"].get(shop, []), savat_updated)
        )
        return STATE_BOZOR_TOVAR_SELECT

    ctx.user_data["bozor_qty"] = qty
    await update.message.reply_text(f"🔢 Miqdor: {qty} dona", reply_markup=dona_keyboard(qty))
    return STATE_BOZOR_TOVAR_DONA

async def bozor_add_name_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    shop = ctx.user_data.get("bozor_shop")

    if text == "🔙 Ortga":
        savat_key = ctx.user_data.get("savat_key")
        savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
        await update.message.reply_text("Tovarni tanlang:", reply_markup=bozor_tovar_keyboard(u["shops"].get(shop, []), savat))
        return STATE_BOZOR_TOVAR_SELECT

    if text not in u["shops"].get(shop, []):
        u["shops"][shop].append(text)
    ctx.user_data["bozor_tovar"] = text
    save_data(data)
    await update.message.reply_text(f"📦 {text} — Birlikni tanlang:", reply_markup=unit_keyboard())
    return STATE_BOZOR_TOVAR_UNIT

async def bozor_del_tovar_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    shop = ctx.user_data.get("bozor_shop")
    savat_key = ctx.user_data.get("savat_key")

    if text == "🔙 Ortga":
        savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
        await update.message.reply_text("Tovarni tanlang:", reply_markup=bozor_tovar_keyboard(u["shops"].get(shop, []), savat))
        return STATE_BOZOR_TOVAR_SELECT

    savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
    new_savat = [s for s in savat if s["name"] != text]
    if len(new_savat) < len(savat):
        u["savat"][savat_key][shop] = new_savat
        save_data(data)
        await send_temp(update, f"✅ '{text}' savatdan o'chirildi!")

    savat_updated = u.get("savat", {}).get(savat_key, {}).get(shop, [])
    await update.message.reply_text("Tovarni tanlang:", reply_markup=bozor_tovar_keyboard(u["shops"].get(shop, []), savat_updated))
    return STATE_BOZOR_TOVAR_SELECT

# ======================== NARXLAR ========================
async def narx_date_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        await update.message.reply_text("🏠 Asosiy menyu:", reply_markup=main_menu_keyboard(u["year"], u["month"]))
        return STATE_MAIN_MENU

    try:
        day = int(text)
        if 1 <= day <= 31:
            savat_key = get_savat_key(u["year"], u["month"], day)
            savat_data = u.get("savat", {}).get(savat_key, {})
            if not savat_data:
                await send_temp(update, f"❌ {format_date(u['year'], u['month'], day)} uchun savatda tovar yo'q.")
                await update.message.reply_text("📅 Sanani tanlang:", reply_markup=numbers_keyboard(31))
                return STATE_NARX_DATE
            ctx.user_data["narx_day"] = day
            ctx.user_data["narx_savat_key"] = savat_key
            shops = list(savat_data.keys())
            rows = [[s] for s in shops]
            rows.append(["🔙 Ortga"])
            await update.message.reply_text(
                f"📅 {format_date(u['year'], u['month'], day)} — Do'konni tanlang:",
                reply_markup=kb(rows)
            )
            return STATE_NARX_SHOP
    except:
        pass

    await update.message.reply_text("📅 Sanani tanlang:", reply_markup=numbers_keyboard(31))
    return STATE_NARX_DATE

async def narx_shop_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        await update.message.reply_text("📅 Sanani tanlang:", reply_markup=numbers_keyboard(31))
        return STATE_NARX_DATE

    savat_key = ctx.user_data.get("narx_savat_key")
    savat_data = u.get("savat", {}).get(savat_key, {})
    shops = list(savat_data.keys())

    if text in shops:
        ctx.user_data["narx_shop"] = text
        savat = savat_data[text]
        rows = []
        for item in savat:
            name = item["name"]
            qty = item["qty"]
            unit = item.get("unit", "kg")
            rows.append([f"💰 {name} ({qty}{unit})"])
        rows.append(["🔙 Ortga"])
        await update.message.reply_text(
            f"🏪 {text} — Narx kiritish uchun tovarni tanlang:",
            reply_markup=kb(rows)
        )
        return STATE_NARX_TOVAR

    rows = [[s] for s in shops]
    rows.append(["🔙 Ortga"])
    await update.message.reply_text("Do'konni tanlang:", reply_markup=kb(rows))
    return STATE_NARX_SHOP

async def narx_tovar_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        savat_key = ctx.user_data.get("narx_savat_key")
        savat_data = u.get("savat", {}).get(savat_key, {})
        shops = list(savat_data.keys())
        rows = [[s] for s in shops]
        rows.append(["🔙 Ortga"])
        await update.message.reply_text("Do'konni tanlang:", reply_markup=kb(rows))
        return STATE_NARX_SHOP

    if text.startswith("💰 "):
        inner = text[2:].strip()
        paren = inner.rfind("(")
        if paren > 0:
            tovar_name = inner[:paren].strip()
        else:
            tovar_name = inner

        shop = ctx.user_data.get("narx_shop")
        savat_key = ctx.user_data.get("narx_savat_key")
        savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
        item = next((s for s in savat if s["name"] == tovar_name), None)

        if item:
            qty = item["qty"]
            unit = item.get("unit", "kg")
            ctx.user_data["narx_tovar"] = tovar_name
            ctx.user_data["narx_qty"] = qty
            ctx.user_data["narx_unit"] = unit

            narxlar = u.get("narxlar", {})
            last_price = None
            all_keys = sorted([k for k in narxlar.keys()
                               if k.startswith(f"{u['year']}-{u['month']:02d}-")], reverse=True)
            day = ctx.user_data.get("narx_day")
            cur_key = get_savat_key(u["year"], u["month"], day)
            for k in all_keys:
                if k == cur_key:
                    continue
                shop_data = narxlar[k].get(shop, {})
                if tovar_name in shop_data:
                    last_price = shop_data[tovar_name].get("price_per_unit")
                    break

            unit_label = "kg" if unit == "kg" else "dona"
            last_str = f"\n⬇️ Oxirgi narx: {last_price:,} so'm/{unit_label}" if last_price else ""
            prompt = (f"📦 {tovar_name}\n"
                      f"{'⚖️' if unit=='kg' else '🔢'} {qty}{unit_label} oldingiz\n"
                      f"💰 1 {unit_label} narxini kiriting:{last_str}")

            rows = [["💰 Narx kiritish"], [f"🔄 {'kg' if unit=='kg' else 'Dona'} o'zgartirish"], ["🔙 Ortga"]]
            await update.message.reply_text(prompt, reply_markup=kb(rows))
            return STATE_NARX_INPUT

    shop = ctx.user_data.get("narx_shop")
    savat_key = ctx.user_data.get("narx_savat_key")
    savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
    rows = [[f"💰 {i['name']} ({i['qty']}{i.get('unit','kg')})"] for i in savat]
    rows.append(["🔙 Ortga"])
    await update.message.reply_text("Tovarni tanlang:", reply_markup=kb(rows))
    return STATE_NARX_TOVAR

async def narx_input_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        shop = ctx.user_data.get("narx_shop")
        savat_key = ctx.user_data.get("narx_savat_key")
        savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
        rows = [[f"💰 {i['name']} ({i['qty']}{i.get('unit','kg')})"] for i in savat]
        rows.append(["🔙 Ortga"])
        await update.message.reply_text("Tovarni tanlang:", reply_markup=kb(rows))
        return STATE_NARX_TOVAR

    if "Narx kiritish" in text or "💰" in text:
        tovar = ctx.user_data.get("narx_tovar")
        qty = ctx.user_data.get("narx_qty")
        unit = ctx.user_data.get("narx_unit", "kg")
        unit_label = "kg" if unit == "kg" else "dona"
        narxlar = u.get("narxlar", {})
        shop = ctx.user_data.get("narx_shop")
        day = ctx.user_data.get("narx_day")
        cur_key = get_savat_key(u["year"], u["month"], day)
        last_price = None
        all_keys = sorted([k for k in narxlar.keys()
                           if k.startswith(f"{u['year']}-{u['month']:02d}-")], reverse=True)
        for k in all_keys:
            if k == cur_key:
                continue
            shop_data = narxlar[k].get(shop, {})
            if tovar in shop_data:
                last_price = shop_data[tovar].get("price_per_unit")
                break
        last_str = f"\n⬇️ Oxirgi narx: {last_price:,} so'm/{unit_label}" if last_price else ""
        await update.message.reply_text(
            f"📦 {tovar} — {qty}{unit_label} oldingiz\n💰 1 {unit_label} narxini kiriting:{last_str}",
            reply_markup=back_keyboard()
        )
        ctx.user_data["narx_awaiting_price"] = True
        return STATE_NARX_INPUT

    if "o'zgartirish" in text or "🔄" in text:
        await update.message.reply_text("Birlikni tanlang:", reply_markup=unit_keyboard())
        return STATE_NARX_UNIT_CHANGE

    if ctx.user_data.get("narx_awaiting_price"):
        try:
            price = float(text.replace(",", "").replace(" ", ""))
            tovar = ctx.user_data.get("narx_tovar")
            qty = ctx.user_data.get("narx_qty")
            unit = ctx.user_data.get("narx_unit", "kg")
            shop = ctx.user_data.get("narx_shop")
            day = ctx.user_data.get("narx_day")
            savat_key = get_savat_key(u["year"], u["month"], day)

            if "narxlar" not in u:
                u["narxlar"] = {}
            if savat_key not in u["narxlar"]:
                u["narxlar"][savat_key] = {}
            if shop not in u["narxlar"][savat_key]:
                u["narxlar"][savat_key][shop] = {}

            total = price * qty
            u["narxlar"][savat_key][shop][tovar] = {
                "price_per_unit": price,
                "qty": qty,
                "unit": unit,
                "total": total
            }
            save_data(data)
            ctx.user_data["narx_awaiting_price"] = False
            unit_label = "kg" if unit == "kg" else "dona"
            await send_temp(update, f"✅ {tovar}: {qty}{unit_label} × {price:,.0f} = {total:,.0f} so'm saqlandi!")

            savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
            rows = [[f"💰 {i['name']} ({i['qty']}{i.get('unit','kg')})"] for i in savat]
            rows.append(["🔙 Ortga"])
            await update.message.reply_text(f"🏪 {shop} — Tovarni tanlang:", reply_markup=kb(rows))
            return STATE_NARX_TOVAR
        except:
            await send_temp(update, "⚠️ Faqat raqam kiriting!")
            return STATE_NARX_INPUT

    tovar = ctx.user_data.get("narx_tovar")
    qty = ctx.user_data.get("narx_qty")
    unit = ctx.user_data.get("narx_unit", "kg")
    unit_label = "kg" if unit == "kg" else "dona"
    rows = [["💰 Narx kiritish"], [f"🔄 {unit_label} o'zgartirish"], ["🔙 Ortga"]]
    await update.message.reply_text(
        f"📦 {tovar} — {qty}{unit_label}\nAmalni tanlang:",
        reply_markup=kb(rows)
    )
    return STATE_NARX_INPUT

async def narx_unit_change_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    tovar = ctx.user_data.get("narx_tovar")

    if text == "🔙 Ortga":
        rows = [["💰 Narx kiritish"], ["🔄 o'zgartirish"], ["🔙 Ortga"]]
        await update.message.reply_text("Amalni tanlang:", reply_markup=kb(rows))
        return STATE_NARX_INPUT

    if "kg" in text.lower():
        ctx.user_data["narx_unit_new"] = "kg"
        ctx.user_data["narx_qty_new"] = 1.0
        await update.message.reply_text(f"⚖️ {tovar} — Miqdorni tanlang (kg):", reply_markup=kg_keyboard(1.0))
        return STATE_NARX_KG_CHANGE

    if "dona" in text.lower():
        ctx.user_data["narx_unit_new"] = "dona"
        ctx.user_data["narx_qty_new"] = 1
        await update.message.reply_text(f"🔢 {tovar} — Donasini tanlang:", reply_markup=dona_keyboard(1))
        return STATE_NARX_DONA_CHANGE

    await update.message.reply_text("Birlikni tanlang:", reply_markup=unit_keyboard())
    return STATE_NARX_UNIT_CHANGE

async def narx_kg_change_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    qty = ctx.user_data.get("narx_qty_new", 1.0)
    tovar = ctx.user_data.get("narx_tovar")
    shop = ctx.user_data.get("narx_shop")
    savat_key = ctx.user_data.get("narx_savat_key")

    if text == "🔙 Ortga":
        await update.message.reply_text("Birlikni tanlang:", reply_markup=unit_keyboard())
        return STATE_NARX_UNIT_CHANGE

    if "➖0.5" in text:
        qty = max(0.5, round(qty - 0.5, 1))
    elif "➕0.5" in text:
        qty = round(qty + 0.5, 1)
    elif "➖10" in text:
        qty = max(0.5, round(qty - 10, 1))
    elif "➕10" in text:
        qty = round(qty + 10, 1)
    elif "Saqlash" in text:
        savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
        for item in savat:
            if item["name"] == tovar:
                item["qty"] = qty
                item["unit"] = "kg"
                break
        save_data(data)
        ctx.user_data["narx_qty"] = qty
        ctx.user_data["narx_unit"] = "kg"
        await send_temp(update, f"✅ {tovar} — {qty}kg ga yangilandi!")
        unit_label = "kg"
        narxlar = u.get("narxlar", {})
        last_price = None
        day = ctx.user_data.get("narx_day")
        cur_key = get_savat_key(u["year"], u["month"], day)
        all_keys = sorted([k for k in narxlar.keys()
                           if k.startswith(f"{u['year']}-{u['month']:02d}-")], reverse=True)
        for k in all_keys:
            if k == cur_key:
                continue
            shop_data = narxlar[k].get(shop, {})
            if tovar in shop_data:
                last_price = shop_data[tovar].get("price_per_unit")
                break
        last_str = f"\n⬇️ Oxirgi narx: {last_price:,} so'm/{unit_label}" if last_price else ""
        rows = [["💰 Narx kiritish"], ["🔄 kg o'zgartirish"], ["🔙 Ortga"]]
        await update.message.reply_text(
            f"📦 {tovar} — {qty}kg oldingiz\n💰 1kg narxini kiriting:{last_str}",
            reply_markup=kb(rows)
        )
        ctx.user_data["narx_awaiting_price"] = True
        return STATE_NARX_INPUT

    ctx.user_data["narx_qty_new"] = qty
    await update.message.reply_text(f"⚖️ Miqdor: {qty}kg", reply_markup=kg_keyboard(qty))
    return STATE_NARX_KG_CHANGE

async def narx_dona_change_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    qty = int(ctx.user_data.get("narx_qty_new", 1))
    tovar = ctx.user_data.get("narx_tovar")
    shop = ctx.user_data.get("narx_shop")
    savat_key = ctx.user_data.get("narx_savat_key")

    if text == "🔙 Ortga":
        await update.message.reply_text("Birlikni tanlang:", reply_markup=unit_keyboard())
        return STATE_NARX_UNIT_CHANGE

    if "➖1" in text:
        qty = max(1, qty - 1)
    elif "➕1" in text:
        qty += 1
    elif "Saqlash" in text:
        savat = u.get("savat", {}).get(savat_key, {}).get(shop, [])
        for item in savat:
            if item["name"] == tovar:
                item["qty"] = qty
                item["unit"] = "dona"
                break
        save_data(data)
        ctx.user_data["narx_qty"] = qty
        ctx.user_data["narx_unit"] = "dona"
        await send_temp(update, f"✅ {tovar} — {qty} donaga yangilandi!")
        narxlar = u.get("narxlar", {})
        last_price = None
        day = ctx.user_data.get("narx_day")
        cur_key = get_savat_key(u["year"], u["month"], day)
        all_keys = sorted([k for k in narxlar.keys()
                           if k.startswith(f"{u['year']}-{u['month']:02d}-")], reverse=True)
        for k in all_keys:
            if k == cur_key:
                continue
            shop_data = narxlar[k].get(shop, {})
            if tovar in shop_data:
                last_price = shop_data[tovar].get("price_per_unit")
                break
        last_str = f"\n⬇️ Oxirgi narx: {last_price:,} so'm/dona" if last_price else ""
        rows = [["💰 Narx kiritish"], ["🔄 Dona o'zgartirish"], ["🔙 Ortga"]]
        await update.message.reply_text(
            f"📦 {tovar} — {qty} dona oldingiz\n💰 1 dona narxini kiriting:{last_str}",
            reply_markup=kb(rows)
        )
        ctx.user_data["narx_awaiting_price"] = True
        return STATE_NARX_INPUT

    ctx.user_data["narx_qty_new"] = qty
    await update.message.reply_text(f"🔢 Miqdor: {qty} dona", reply_markup=dona_keyboard(qty))
    return STATE_NARX_DONA_CHANGE

# ======================== HISOBOT ========================
async def hisobot_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        await update.message.reply_text("🏠 Asosiy menyu:", reply_markup=main_menu_keyboard(u["year"], u["month"]))
        return STATE_MAIN_MENU

    try:
        parts = text.split(".")
        if len(parts) == 3:
            day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
            savat_key = get_savat_key(year, month, day)
            narxlar = u.get("narxlar", {})
            day_narx = narxlar.get(savat_key, {})

            if not day_narx:
                await send_temp(update, "❌ Bu sana uchun ma'lumot yo'q.")
                return STATE_HISOBOT

            report = f"📊 {text} yil uchun Hisobot\n{'═'*30}\n"
            grand_total = 0
            for shop, items in day_narx.items():
                shop_total = 0
                report += f"\n🏪 {shop}:\n"
                for tname, tdata in items.items():
                    price = tdata.get("price_per_unit", 0)
                    qty = tdata.get("qty", 1)
                    unit = tdata.get("unit", "kg")
                    total = tdata.get("total", price * qty)
                    unit_label = "kg" if unit == "kg" else "dona"
                    report += f"  • {tname} ({qty}{unit_label}) — {price:,.0f}×{qty}={total:,.0f} so'm\n"
                    shop_total += total
                report += f"  💰 {shop} jami: {shop_total:,.0f} so'm\n"
                grand_total += shop_total
            report += f"\n{'═'*30}\n💵 Umumiy jami: {grand_total:,.0f} so'm"

            await update.message.reply_text(report, reply_markup=main_menu_keyboard(u["year"], u["month"]))
            return STATE_MAIN_MENU
    except:
        pass

    narxlar = u.get("narxlar", {})
    dates = [k for k in narxlar.keys() if k.startswith(f"{u['year']}-{u['month']:02d}-")]
    display = [format_date(*parse_date_key(d)) for d in sorted(dates)]
    await update.message.reply_text("Sanani tanlang:", reply_markup=dates_keyboard(display))
    return STATE_HISOBOT

# ======================== STATISTIKA ========================
async def statistika_show(update: Update, u):
    narxlar = u.get("narxlar", {})
    all_keys = sorted(narxlar.keys())

    if len(all_keys) < 1:
        await update.message.reply_text("❌ Statistika uchun kamida 1 ta ma'lumot kerak.")
        return

    last_key = all_keys[-1]
    prev_keys = all_keys[:-1][-2:]
    last_data = narxlar[last_key]
    last_date = format_date(*parse_date_key(last_key))

    msg = f"📈 Statistika — {last_date}\n{'═'*30}\n"
    oshgan = []
    tushgan = []

    for shop, items in last_data.items():
        msg += f"\n🏪 {shop}:\n"
        for tname, tdata in items.items():
            cur_price = tdata.get("price_per_unit", 0)
            unit = tdata.get("unit", "kg")
            unit_label = "kg" if unit == "kg" else "dona"

            prev_prices = []
            prev_dates_str = []
            for pk in prev_keys:
                prev_shop = narxlar.get(pk, {}).get(shop, {})
                if tname in prev_shop:
                    prev_prices.append(prev_shop[tname].get("price_per_unit", 0))
                    prev_dates_str.append(format_date(*parse_date_key(pk)))

            if not prev_prices:
                icon = "🆕"
                prev_str = "yangi tovar"
            else:
                is_up = any(cur_price > p for p in prev_prices)
                is_equal = all(cur_price == p for p in prev_prices)
                if is_up:
                    icon = "❌↗️"
                    oshgan.append(f"{tname}")
                elif is_equal:
                    icon = "✅"
                else:
                    icon = "⭕️↘️"
                    tushgan.append(f"{tname}")
                prev_str = " / ".join([f"{d}: {p:,.0f}" for d, p in zip(prev_dates_str, prev_prices)])

            msg += f"  {icon} {tname}: {cur_price:,.0f} so'm/{unit_label}"
            if prev_prices:
                msg += f"\n      ({prev_str})\n"
            else:
                msg += "\n"

    if oshgan:
        msg += f"\n❌↗️ Oshgan: {', '.join(oshgan)}"
    if tushgan:
        msg += f"\n⭕️↘️ Tushgan: {', '.join(tushgan)}"

    await update.message.reply_text(msg)

# ======================== UNDO ========================
async def undo_date_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)

    if text == "🔙 Ortga":
        await update.message.reply_text("🏠 Asosiy menyu:", reply_markup=main_menu_keyboard(u["year"], u["month"]))
        return STATE_MAIN_MENU

    try:
        parts = text.split(".")
        if len(parts) == 3:
            day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
            savat_key = get_savat_key(year, month, day)
            narxlar = u.get("narxlar", {})
            day_narx = narxlar.get(savat_key, {})

            if not day_narx:
                await send_temp(update, "❌ Bu sana uchun ma'lumot yo'q.")
                return STATE_UNDO_DATE

            last_shop = None
            last_tovar = None
            for shop in list(day_narx.keys()):
                for tname in list(day_narx[shop].keys()):
                    last_shop = shop
                    last_tovar = tname

            if last_shop and last_tovar:
                del u["narxlar"][savat_key][last_shop][last_tovar]
                if not u["narxlar"][savat_key][last_shop]:
                    del u["narxlar"][savat_key][last_shop]
                if not u["narxlar"][savat_key]:
                    del u["narxlar"][savat_key]
                save_data(data)
                await send_temp(update, f"✅ {last_shop} → '{last_tovar}' narxi o'chirildi!")
            else:
                await send_temp(update, "❌ O'chirish uchun narx topilmadi.")

            narxlar = u.get("narxlar", {})
            dates = [k for k in narxlar.keys() if k.startswith(f"{u['year']}-{u['month']:02d}-")]
            if dates:
                display = [format_date(*parse_date_key(d)) for d in sorted(dates)]
                await update.message.reply_text(
                    "🗑 Yana o'chirish uchun sana tanlang yoki ortga qayting:",
                    reply_markup=dates_keyboard(display)
                )
                return STATE_UNDO_DATE
            else:
                await update.message.reply_text("🏠 Asosiy menyu:", reply_markup=main_menu_keyboard(u["year"], u["month"]))
                return STATE_MAIN_MENU
    except:
        pass

    narxlar = u.get("narxlar", {})
    dates = [k for k in narxlar.keys() if k.startswith(f"{u['year']}-{u['month']:02d}-")]
    display = [format_date(*parse_date_key(d)) for d in sorted(dates)]
    await update.message.reply_text("Sanani tanlang:", reply_markup=dates_keyboard(display))
    return STATE_UNDO_DATE

# ======================== CANCEL ========================
async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    data = load_data()
    uid = update.effective_user.id
    u = get_user(data, uid)
    await update.message.reply_text("🏠 Asosiy menyu:", reply_markup=main_menu_keyboard(u["year"], u["month"]))
    return STATE_MAIN_MENU

# ======================== MAIN ========================
def main():
    import asyncio
    asyncio.set_event_loop(asyncio.new_event_loop())
    
    TOKEN = "8213220755:AAEitj0sJeIHU7D1q_Hs63nRO-3SO22I32I"

    app = Application.builder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            STATE_YEAR_MONTH:        [MessageHandler(filters.TEXT & ~filters.COMMAND, year_month_handler)],
            STATE_MAIN_MENU:         [MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu_handler)],
            STATE_ROYXAT_MENU:       [MessageHandler(filters.TEXT & ~filters.COMMAND, royxat_menu_handler)],
            STATE_ROYXAT_ADD_SHOP:   [MessageHandler(filters.TEXT & ~filters.COMMAND, royxat_add_shop)],
            STATE_ROYXAT_DEL_SHOP:   [MessageHandler(filters.TEXT & ~filters.COMMAND, royxat_del_shop)],
            STATE_ROYXAT_SHOP:       [MessageHandler(filters.TEXT & ~filters.COMMAND, royxat_shop_handler)],
            STATE_ROYXAT_TOVAR_ADD:  [MessageHandler(filters.TEXT & ~filters.COMMAND, royxat_tovar_add)],
            STATE_ROYXAT_TOVAR_DEL:  [MessageHandler(filters.TEXT & ~filters.COMMAND, royxat_tovar_del)],
            STATE_BOZOR_DATE:        [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_date_handler)],
            STATE_BOZOR_SHOP:        [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_shop_handler)],
            STATE_BOZOR_TOVAR_SELECT:[MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_tovar_select_handler)],
            STATE_BOZOR_TOVAR_UNIT:  [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_tovar_unit_handler)],
            STATE_BOZOR_TOVAR_KG:    [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_tovar_kg_handler)],
            STATE_BOZOR_TOVAR_DONA:  [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_tovar_dona_handler)],
            STATE_BOZOR_ADD_NAME:    [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_add_name_handler)],
            STATE_BOZOR_DEL_TOVAR:   [MessageHandler(filters.TEXT & ~filters.COMMAND, bozor_del_tovar_handler)],
            STATE_SAVAT_NARX_DATE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, savat_narx_date_handler)],
            STATE_NARX_DATE:         [MessageHandler(filters.TEXT & ~filters.COMMAND, narx_date_handler)],
            STATE_NARX_SHOP:         [MessageHandler(filters.TEXT & ~filters.COMMAND, narx_shop_handler)],
            STATE_NARX_TOVAR:        [MessageHandler(filters.TEXT & ~filters.COMMAND, narx_tovar_handler)],
            STATE_NARX_INPUT:        [MessageHandler(filters.TEXT & ~filters.COMMAND, narx_input_handler)],
            STATE_NARX_UNIT_CHANGE:  [MessageHandler(filters.TEXT & ~filters.COMMAND, narx_unit_change_handler)],
            STATE_NARX_KG_CHANGE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, narx_kg_change_handler)],
            STATE_NARX_DONA_CHANGE:  [MessageHandler(filters.TEXT & ~filters.COMMAND, narx_dona_change_handler)],
            STATE_HISOBOT:           [MessageHandler(filters.TEXT & ~filters.COMMAND, hisobot_handler)],
            STATE_UNDO_DATE:         [MessageHandler(filters.TEXT & ~filters.COMMAND, undo_date_handler)],
        },
        fallbacks=[CommandHandler("cancel", cancel), CommandHandler("start", start)],
        per_message=False,
    )

    app.add_handler(conv)
    print("🤖 Bot ishga tushdi...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
