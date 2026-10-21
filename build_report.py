#!/usr/bin/env python3
"""Build a compact, evidence-backed PDF report from the measured JSON outputs."""
import json
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"
OUT = ROOT / "vacancies_python_report.pdf"


def read(name):
    path = RESULTS / name
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def main():
    mr = read("mapreduce_10000.json") or read("mapreduce_1000.json")
    sql = read("duckdb.json")
    audit = read("audit.json")
    if not mr:
        raise SystemExit("MapReduce output missing")
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="TitleRu", parent=styles["Title"], alignment=TA_CENTER, fontName="DejaVu", fontSize=19, leading=23))
    styles.add(ParagraphStyle(name="BodyRu", parent=styles["BodyText"], fontName="DejaVu", fontSize=9.3, leading=13, spaceAfter=6))
    styles.add(ParagraphStyle(name="HeadRu", parent=styles["Heading2"], fontName="DejaVu-Bold", fontSize=13, leading=16, spaceBefore=10, spaceAfter=5))
    # Register fonts available on standard Ubuntu installations.
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    pdfmetrics.registerFont(TTFont("DejaVu", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"))
    pdfmetrics.registerFont(TTFont("DejaVu-Bold", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"))
    doc = SimpleDocTemplate(str(OUT), pagesize=A4, rightMargin=17*mm, leftMargin=17*mm, topMargin=15*mm, bottomMargin=15*mm)
    story = [Paragraph("Анализ Python-вакансий 2024", styles["TitleRu"]),
        Paragraph("RustFS · потоковый MapReduce · DuckDB", styles["BodyRu"]),
        Paragraph("В файле содержится 10 205 102 записи вакансий и строка заголовка. Исходный CSV размером 24 970 431 974 байта не изменялся; для расчётов в RustFS хранится gzip-представление.", styles["BodyRu"]),
        Paragraph("Вариант и правило выборки", styles["HeadRu"]),
        Paragraph("Вариант: Python. В сегмент включена строка, если название name содержит python без учёта регистра либо отдельный токен python присутствует в списке key_skills, разделённом точкой с запятой. Описание вакансии не анализировалось. Навыки нормализованы в нижний регистр, повторы внутри строки удалены; пары и тройки — неупорядоченные сочетания.", styles["BodyRu"]),
        Paragraph("Результаты обработки", styles["HeadRu"])]
    rows = [["Показатель", "Результат"], ["Строк CSV", f"{mr['input_rows']:,}".replace(",", " ")],
        ["Python-сегмент", f"{mr['python_vacancies']:,}".replace(",", " ")],
        ["Подсчёт навыков", f"{sum(mr['counts']['1'].values()):,}".replace(",", " ")],
        ["Счётчики пар / троек", f"{len(mr['counts']['2']):,} / {len(mr['counts']['3']):,} уникальных комбинаций"],
        ["Аудит MapReduce ↔ DuckDB", audit.get("status", "не выполнен") if audit else "не выполнен"]]
    t = Table(rows, colWidths=[68*mm, 104*mm], repeatRows=1)
    t.setStyle(TableStyle([("FONTNAME", (0,0),(-1,-1),"DejaVu"),("BACKGROUND",(0,0),(-1,0),colors.HexColor("#17324d")),("TEXTCOLOR",(0,0),(-1,0),colors.white),("GRID",(0,0),(-1,-1),0.35,colors.HexColor("#9aa7b2")),("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,colors.HexColor("#eef3f7")]),("FONTSIZE",(0,0),(-1,-1),8.5),("VALIGN",(0,0),(-1,-1),"TOP"),("PADDING",(0,0),(-1,-1),5)]))
    story.extend([t, Paragraph("Наиболее частые навыки", styles["HeadRu"])])
    for level, title in (("1", "Одиночные навыки"), ("2", "Пары"), ("3", "Тройки")):
        story.append(Paragraph(title, styles["BodyRu"]))
        src = (sql or mr)["counts"][level]
        top = sorted(src.items(), key=lambda x: (-x[1], x[0]))[:10]
        tb = Table([["Комбинация", "Вакансий"]] + [[k, f"{v:,}".replace(",", " ")] for k,v in top], colWidths=[135*mm,37*mm], repeatRows=1)
        tb.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),"DejaVu"),("BACKGROUND",(0,0),(-1,0),colors.HexColor("#dce7ef")),("GRID",(0,0),(-1,-1),0.25,colors.grey),("FONTSIZE",(0,0),(-1,-1),8),("PADDING",(0,0),(-1,-1),4)]))
        story.append(tb)
    story.append(Paragraph("Региональный срез", styles["HeadRu"]))
    rr = [["Регион/area", "Вакансий", "Зарплат с RUR", "Медиана, ₽"]]
    for area in mr.get("top_regions", []):
        med = area["median_salary_rur"]
        rr.append([area["area"], str(area["vacancies"]), str(area["salary_observations"]), f"{med:,.0f}".replace(",", " ") if med else "н/д"])
    rt = Table(rr, colWidths=[67*mm,35*mm,35*mm,35*mm], repeatRows=1)
    rt.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),"DejaVu"),("BACKGROUND",(0,0),(-1,0),colors.HexColor("#17324d")),("TEXTCOLOR",(0,0),(-1,0),colors.white),("GRID",(0,0),(-1,-1),0.3,colors.grey),("FONTSIZE",(0,0),(-1,-1),8),("PADDING",(0,0),(-1,-1),4)]))
    story.append(rt)
    story.append(Paragraph("Зарплатный эффект навыковых сочетаний", styles["HeadRu"]))
    story.append(Paragraph("Сравниваются медиана зарплаты вакансий с парой/тройкой навыков и наибольшая медиана среди входящих в сочетание одиночных навыков. Рассматривались только суммы в RUR; диапазон представлен серединой границ, односторонняя сумма — указанной границей. Это описательное сравнение, не причинная оценка. Значения приводятся для 25 наиболее частых пар и 25 троек.", styles["BodyRu"]))
    if sql and sql.get("salary_impact_top_combinations"):
        si = [["Сочетание", "N", "Медиана ₽", "К лучшему одиночному навыку"]]
        for x in sql["salary_impact_top_combinations"][:12]:
            si.append([x["combo"], str(x["vacancies"]), f"{x['median_salary_rur']:,.0f}".replace(","," ") if x["median_salary_rur"] else "н/д", f"{x['premium_percent']:+.1f}%" if x["premium_percent"] is not None else "н/д"])
        st = Table(si, colWidths=[75*mm,20*mm,35*mm,42*mm], repeatRows=1)
        st.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),"DejaVu"),("BACKGROUND",(0,0),(-1,0),colors.HexColor("#dce7ef")),("GRID",(0,0),(-1,-1),0.25,colors.grey),("FONTSIZE",(0,0),(-1,-1),7),("PADDING",(0,0),(-1,-1),3)]))
        story.append(st)
    story.append(Paragraph("Производительность и контроль", styles["HeadRu"]))
    profiles = []
    for p in sorted(RESULTS.glob("mapreduce_*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        profiles.append([str(d["chunk_rows"]), f"{d['elapsed_seconds']:.1f}", f"{d['peak_rss_kib']/1024:.1f}"])
    if profiles:
        pt = Table([["Строк в блоке", "Время, с", "Пиковая RSS, MiB"]]+profiles, colWidths=[58*mm,58*mm,58*mm])
        pt.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),"DejaVu"),("BACKGROUND",(0,0),(-1,0),colors.HexColor("#dce7ef")),("GRID",(0,0),(-1,-1),0.25,colors.grey),("FONTSIZE",(0,0),(-1,-1),8),("PADDING",(0,0),(-1,-1),4)]))
        story.append(pt)
    story.append(Paragraph("Тест отказа mapper: искусственная ошибка должна завершать задачу с ошибкой; частичный результат не публикуется. MapReduce и SQL используют один S3 gzip-объект и единый сегмент; аудит сверяет словари одиночных навыков, пар и троек и число вакансий. В качестве оптимизации локальные счётчики блока редуцируются агрегированно, уменьшая количество операций записи в итоговые глобальные счётчики.", styles["BodyRu"]))
    story.append(Paragraph("Ограничения: RustFS использует восемь каталогов на одном физическом NVMe (учебная конфигурация, без отказоустойчивости к потере диска). Зарплата указана только при валюте RUR, пропуски не импутируются. CSV является снимком 2024 года.", styles["BodyRu"]))
    doc.build(story)
    print(OUT)


if __name__ == "__main__":
    main()
