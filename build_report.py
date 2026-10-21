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
    ingest = read("ingest_verification.json")
    optimization = read("optimization_benchmark.json")
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
        Paragraph("В файле содержится 10 205 102 записи вакансий и строка заголовка. Исходный CSV размером 24 970 431 974 байта не изменялся; gzip-объект в RustFS подтверждён полным чтением и SHA-256.", styles["BodyRu"]),
        Paragraph("Вариант и правило выборки", styles["HeadRu"]),
        Paragraph("Вариант: Python. В сегмент включена строка, если название name содержит python без учёта регистра либо отдельный токен python присутствует в списке key_skills, разделённом точкой с запятой. Описание вакансии не анализировалось. Навыки нормализованы в нижний регистр, повторы внутри строки удалены; пары и тройки — неупорядоченные сочетания.", styles["BodyRu"]),
        Paragraph("Результаты обработки", styles["HeadRu"]) ]
    if not mr:
        story.extend([Paragraph("Статус на момент сборки: подтверждена загрузка и целостность исходного набора, опубликован воспроизводимый код MapReduce и DuckDB, выполнены два unit-теста и тест искусственного отказа mapper. Полный аналитический проход по 10,2 млн строк и сверка двух движков ещё выполняются; численные аналитические выводы здесь намеренно не приводятся.", styles["BodyRu"]),
            Paragraph("Проверка объекта", styles["HeadRu"])])
        if ingest:
            meta = ingest["object"]
            story.append(Paragraph(f"{meta['uri']} · {meta['bytes']:,} байт · SHA-256 {meta['sha256']} · проверен потоковым чтением целиком.".replace(",", " "), styles["BodyRu"]))
        story.append(Paragraph("Сегмент и алгоритм", styles["HeadRu"]))
        story.append(Paragraph("Вариант Python: name содержит Python без учёта регистра либо отдельный токен Python присутствует в key_skills. Описание не анализируется. Mapper считает уникальные навыки строки и их неупорядоченные пары и тройки; reducer объединяет локальные счётчики блоков. Команда полного запуска и независимого SQL-аудита приведена в README репозитория.", styles["BodyRu"]))
        doc.build(story)
        print(OUT)
        return
    rows = [["Показатель", "Результат"], ["Строк CSV", f"{mr['input_rows']:,}".replace(",", " ")],
        ["Python-сегмент", f"{mr['python_vacancies']:,}".replace(",", " ")],
        ["Уникальные навыки / пары / тройки", f"{len(mr['counts']['1']):,} / {len(mr['counts']['2']):,} / {len(mr['counts']['3']):,}".replace(",", " ")],
        ["Всего упоминаний навыков / пар / троек", " / ".join(f"{sum(mr['counts'][k].values()):,}".replace(",", " ") for k in ("1","2","3"))],
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
    story.append(Paragraph("Рост числа сочетаний", styles["HeadRu"]))
    hist = mr.get("skill_count_histogram", {})
    avg = mr["python_vacancies"] or 1
    hist_text = ", ".join(f"{k} навыков — {v:,}".replace(",", " ") for k,v in sorted(hist.items(), key=lambda x:int(x[0])))
    story.append(Paragraph(f"В сегменте: {sum(mr['counts']['1'].values()):,} одиночных упоминаний, {sum(mr['counts']['2'].values()):,} парных и {sum(mr['counts']['3'].values()):,} тройных сочетаний. Вакансия с k уникальными навыками даёт k, C(k,2) и C(k,3) комбинаций; распределение числа навыков: {hist_text}. Среднее k={sum(int(k)*v for k,v in hist.items())/avg:.2f}.", styles["BodyRu"]))
    story.append(Paragraph("Региональный срез: три лидирующих региона", styles["HeadRu"]))
    region_data = sql.get("top_regions", []) if sql else []
    if not region_data:
        region_data = mr.get("top_regions", [])
    rr = [["Регион/area", "Вакансий", "Зарплат с RUR", "Медиана, ₽"]]
    for area in region_data:
        med = area["median_salary_rur"]
        rr.append([area["area"], str(area["vacancies"]), str(area["salary_observations"]), f"{med:,.0f}".replace(",", " ") if med else "н/д"])
    rt = Table(rr, colWidths=[67*mm,35*mm,35*mm,35*mm], repeatRows=1)
    rt.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),"DejaVu"),("BACKGROUND",(0,0),(-1,0),colors.HexColor("#17324d")),("TEXTCOLOR",(0,0),(-1,0),colors.white),("GRID",(0,0),(-1,-1),0.3,colors.grey),("FONTSIZE",(0,0),(-1,-1),8),("PADDING",(0,0),(-1,-1),4)]))
    story.append(rt)
    for area in region_data:
        story.append(Paragraph(f"{area['area']}: востребованные навыки", styles["BodyRu"]))
        table_rows = [["Навыки/сочетания", "Вакансий"]]
        for key in ("top_skills", "top_pairs", "top_triples"):
            table_rows.extend([[x["combo"], str(x["vacancies"])] for x in area.get(key, [])[:5]])
        reg_table = Table(table_rows, colWidths=[135*mm,37*mm], repeatRows=1)
        reg_table.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),"DejaVu"),("BACKGROUND",(0,0),(-1,0),colors.HexColor("#dce7ef")),("GRID",(0,0),(-1,-1),0.25,colors.grey),("FONTSIZE",(0,0),(-1,-1),7.5),("PADDING",(0,0),(-1,-1),3)]))
        story.append(reg_table)
    story.append(Paragraph("Зарплатный эффект навыковых сочетаний", styles["HeadRu"]))
    story.append(Paragraph("Только валюта RUR; диапазон зарплаты заменён серединой границ, для одностороннего диапазона используется заданная граница. В выборку сочетаний вошли пары/тройки с минимум 30 вакансиями и 20 известными зарплатами. База — медиана наиболее часто встречающегося одиночного навыка из сочетания в Python-сегменте; Δ = (медиана сочетания / медиана базы − 1) × 100%. Это описательная, не причинная оценка.", styles["BodyRu"]))
    if sql:
        for label, key in (("Наибольший рост медианы", "top_salary_increases"), ("Наибольшее снижение медианы", "top_salary_decreases")):
            story.append(Paragraph(label, styles["BodyRu"]))
            si = [["Сочетание", "N", "Медиана ₽", "База навыка", "Δ"]]
            for x in sql.get(key, []):
                si.append([x["combo"], str(x["vacancies"]), f"{x['median_salary_rur']:,.0f}".replace(","," ") if x["median_salary_rur"] else "н/д", x.get("baseline_skill_by_segment_frequency") or "н/д", f"{x['premium_percent']:+.1f}%" if x["premium_percent"] is not None else "н/д"])
            st = Table(si, colWidths=[57*mm,16*mm,31*mm,48*mm,20*mm], repeatRows=1)
            st.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),"DejaVu"),("BACKGROUND",(0,0),(-1,0),colors.HexColor("#dce7ef")),("GRID",(0,0),(-1,-1),0.25,colors.grey),("FONTSIZE",(0,0),(-1,-1),6.5),("PADDING",(0,0),(-1,-1),3)]))
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
    if sql:
        story.append(Paragraph(f"DuckDB: {sql['elapsed_seconds']:.1f} с, пиковая RSS {sql['peak_rss_kib']/1024:.1f} MiB.", styles["BodyRu"]))
    if optimization:
        story.append(Paragraph("Бонус: до/после оптимизации предварительного фильтра", styles["HeadRu"]))
        before,after=optimization["before"]["metrics"],optimization["after"]["metrics"]
        bt=Table([["Метод", "Медиана 5 запусков, с", "Пик временных аллокаций, байт"],
            ["До: нормализация set/sort навыков",f"{before['median_seconds']:.4f}",str(before["peak_temporary_bytes"])],
            ["После: ранний title + прямой токен",f"{after['median_seconds']:.4f}",str(after["peak_temporary_bytes"])],
            ["Ускорение",f"{optimization['time_improvement_percent']:+.1f}%","выборки совпали"]],colWidths=[76*mm,47*mm,49*mm])
        bt.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),"DejaVu"),("BACKGROUND",(0,0),(-1,0),colors.HexColor("#dce7ef")),("GRID",(0,0),(-1,-1),0.25,colors.grey),("FONTSIZE",(0,0),(-1,-1),7.5),("PADDING",(0,0),(-1,-1),4)]))
        story.append(bt)
        story.append(Paragraph(f"Микробенчмарк выполнен на первых {optimization['sample_input_rows']:,} строках того же S3-объекта, по {optimization['repetitions_per_method']} повтора для каждого варианта. Метрика пика — временные Python-аллокации, замеренные tracemalloc; полная пиковая RSS для обработки датасета приведена отдельно выше.".replace(",", " "),styles["BodyRu"]))
    story.append(Paragraph("Тест отказа mapper: искусственная ошибка должна завершать задачу с ошибкой; частичный результат не публикуется. MapReduce и SQL используют один S3 gzip-объект и единый сегмент; аудит сверяет словари одиночных навыков, пар и троек и число вакансий. В качестве оптимизации локальные счётчики блока редуцируются агрегированно, уменьшая количество операций записи в итоговые глобальные счётчики.", styles["BodyRu"]))
    story.append(Paragraph("Ограничения: RustFS использует восемь каталогов на одном физическом NVMe (учебная конфигурация, без отказоустойчивости к потере диска). Зарплата указана только при валюте RUR, пропуски не импутируются. CSV является снимком 2024 года.", styles["BodyRu"]))
    for img in (ROOT / "evidence" / "rustfs_buckets.png", ROOT / "evidence" / "rustfs_hh_lake_object.png"):
        if img.exists():
            story.extend([PageBreak(), Paragraph(f"Скриншот консоли RustFS: {img.stem.replace('_',' ')}", styles["HeadRu"]), Spacer(1,4*mm)])
            from reportlab.platypus import Image
            story.append(Image(str(img), width=170*mm, height=95*mm, kind="proportional"))
    doc.build(story)
    print(OUT)


if __name__ == "__main__":
    main()
