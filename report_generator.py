"""
Data Quality Assessment PDF Report Generator
Produces a professional, governance-grade PDF report for Snowflake object test results.
Dependencies: reportlab, matplotlib
"""

import io
import os
import math
from datetime import datetime

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend — must be set before any pyplot import
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether, PageBreak, Image
)
from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.pdfgen import canvas as pdfcanvas

# ── Brand colours ────────────────────────────────────────────────────────────
SNOW_DARK   = colors.HexColor('#0D1B2A')   # deep navy
SNOW_BLUE   = colors.HexColor('#1E6FA5')   # Snowflake blue
SNOW_CYAN   = colors.HexColor('#29B5E8')   # accent cyan
SNOW_LIGHT  = colors.HexColor('#EBF5FB')   # light background
PASS_GREEN  = colors.HexColor('#27AE60')
FAIL_RED    = colors.HexColor('#E74C3C')
SKIP_GREY   = colors.HexColor('#95A5A6')
WARN_AMBER  = colors.HexColor('#F39C12')
TEXT_DARK   = colors.HexColor('#2C3E50')
TEXT_MID    = colors.HexColor('#566573')
TEXT_LIGHT  = colors.HexColor('#839098')
ROW_ALT     = colors.HexColor('#F4F8FB')
WHITE       = colors.white

# ── Matplotlib colours (hex strings) ─────────────────────────────────────────
MP_PASS   = '#27AE60'
MP_FAIL   = '#E74C3C'
MP_SKIP   = '#95A5A6'
MP_BLUE   = '#1E6FA5'
MP_CYAN   = '#29B5E8'
MP_AMBER  = '#F39C12'
MP_DARK   = '#0D1B2A'


# ── Styles ───────────────────────────────────────────────────────────────────

def _styles():
    base = getSampleStyleSheet()
    def ps(name, **kw):
        return ParagraphStyle(name, **kw)

    return {
        'cover_title': ps('cover_title',
            fontName='Helvetica-Bold', fontSize=32, textColor=WHITE,
            leading=40, alignment=TA_CENTER),
        'cover_sub': ps('cover_sub',
            fontName='Helvetica', fontSize=14, textColor=colors.HexColor('#BDC3C7'),
            leading=20, alignment=TA_CENTER),
        'cover_meta': ps('cover_meta',
            fontName='Helvetica', fontSize=11, textColor=WHITE,
            leading=16, alignment=TA_CENTER),
        'section_h': ps('section_h',
            fontName='Helvetica-Bold', fontSize=16, textColor=SNOW_BLUE,
            leading=22, spaceBefore=18, spaceAfter=8),
        'sub_h': ps('sub_h',
            fontName='Helvetica-Bold', fontSize=12, textColor=TEXT_DARK,
            leading=16, spaceBefore=10, spaceAfter=4),
        'body': ps('body',
            fontName='Helvetica', fontSize=10, textColor=TEXT_DARK,
            leading=14),
        'body_small': ps('body_small',
            fontName='Helvetica', fontSize=9, textColor=TEXT_MID,
            leading=12),
        'code': ps('code',
            fontName='Courier', fontSize=8, textColor=TEXT_DARK,
            backColor=colors.HexColor('#F2F3F4'),
            leading=11, leftIndent=6, rightIndent=6,
            borderPad=4),
        'pass': ps('pass',
            fontName='Helvetica-Bold', fontSize=10, textColor=PASS_GREEN),
        'fail': ps('fail',
            fontName='Helvetica-Bold', fontSize=10, textColor=FAIL_RED),
        'skip': ps('skip',
            fontName='Helvetica-Bold', fontSize=10, textColor=SKIP_GREY),
        'kpi_val': ps('kpi_val',
            fontName='Helvetica-Bold', fontSize=28, textColor=SNOW_BLUE,
            leading=32, alignment=TA_CENTER),
        'kpi_lbl': ps('kpi_lbl',
            fontName='Helvetica', fontSize=10, textColor=TEXT_MID,
            leading=12, alignment=TA_CENTER),
        'footer': ps('footer',
            fontName='Helvetica', fontSize=8, textColor=TEXT_LIGHT,
            leading=10, alignment=TA_CENTER),
        'recommendation': ps('recommendation',
            fontName='Helvetica', fontSize=10, textColor=TEXT_DARK,
            leading=14, leftIndent=15, bulletIndent=5),
    }


# ── Chart helpers ─────────────────────────────────────────────────────────────

def _fig_to_image(fig, width_cm=14, height_cm=8):
    """Convert a matplotlib figure to a ReportLab Image flowable."""
    buf = io.BytesIO()
    fig.savefig(buf, format='PNG', dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close(fig)
    buf.seek(0)
    img = Image(buf, width=width_cm * cm, height=height_cm * cm)
    return img


def _donut_chart(passed, failed, not_run, score):
    """Donut chart showing pass/fail/not-run + health score in the hole."""
    total = passed + failed + not_run
    if total == 0:
        total = 1

    sizes  = [max(passed, 0.0001), max(failed, 0.0001), max(not_run, 0.0001)]
    clrs   = [MP_PASS, MP_FAIL, MP_SKIP]
    labels = [f'Passed\n{passed}', f'Failed\n{failed}', f'Not Run\n{not_run}']

    fig, ax = plt.subplots(figsize=(5, 5))
    wedges, _ = ax.pie(
        sizes, colors=clrs, startangle=90,
        wedgeprops=dict(width=0.52, edgecolor='white', linewidth=2),
        counterclock=False
    )

    # Health score in centre
    score_color = MP_PASS if score >= 80 else (MP_AMBER if score >= 50 else MP_FAIL)
    ax.text(0, 0.08, f'{score:.0f}%', ha='center', va='center',
            fontsize=26, fontweight='bold', color=score_color)
    ax.text(0, -0.22, 'Health Score', ha='center', va='center',
            fontsize=9, color='#566573')

    legend_patches = [
        mpatches.Patch(color=c, label=l) for c, l in
        zip(clrs, [f'Passed ({passed})', f'Failed ({failed})', f'Not Run ({not_run})'])
    ]
    ax.legend(handles=legend_patches, loc='lower center',
              bbox_to_anchor=(0.5, -0.15), ncol=3, frameon=False,
              fontsize=9)
    ax.set_title('Test Results Distribution', fontsize=12,
                 fontweight='bold', color=MP_DARK, pad=12)
    ax.axis('equal')
    fig.patch.set_facecolor('white')
    return _fig_to_image(fig, width_cm=9, height_cm=9)


def _bar_chart_tests(test_cases):
    """Horizontal bar chart — one bar per test case coloured by result."""
    names   = []
    colours = []
    vals    = []

    for i, t in enumerate(test_cases):
        name = t.get('test_name', f'Test {i+1}')
        # Truncate long names
        name = name[:42] + '…' if len(name) > 42 else name
        names.append(name)
        status = str(t.get('status', 'NOT_RUN')).upper()
        if status == 'PASSED':
            colours.append(MP_PASS)
            vals.append(1)
        elif status == 'FAILED':
            colours.append(MP_FAIL)
            vals.append(1)
        else:
            colours.append(MP_SKIP)
            vals.append(1)

    n = len(names)
    fig_h = max(3.5, n * 0.45)
    fig, ax = plt.subplots(figsize=(11, fig_h))

    y_pos = list(range(n))
    bars  = ax.barh(y_pos, vals, color=colours, edgecolor='white',
                    linewidth=0.5, height=0.6)

    # Status labels on bars
    status_labels = []
    for t in test_cases:
        s = str(t.get('status', 'NOT RUN')).upper().replace('_', ' ')
        status_labels.append(s)

    for i, (bar, lbl) in enumerate(zip(bars, status_labels)):
        ax.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height() / 2,
                lbl, va='center', fontsize=8,
                color=colours[i], fontweight='bold')

    ax.set_yticks(y_pos)
    ax.set_yticklabels(names, fontsize=8)
    ax.set_xlim(0, 1.6)
    ax.set_xticks([])
    ax.invert_yaxis()
    ax.set_title('Individual Test Case Results', fontsize=12,
                 fontweight='bold', color=MP_DARK, pad=10)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    fig.patch.set_facecolor('white')
    fig.tight_layout()
    return _fig_to_image(fig, width_cm=17, height_cm=max(5, fig_h * 1.2))


def _stats_bar_chart(statistics):
    """Grouped bar chart for numeric column statistics."""
    if not statistics:
        return None

    cols = list(statistics.keys())[:6]
    min_vals = []
    avg_vals = []
    max_vals = []

    for c in cols:
        s = statistics[c]
        def _f(k):
            v = s.get(k) or s.get(k.lower()) or s.get(k.upper())
            try:
                return float(v) if v is not None else 0.0
            except (TypeError, ValueError):
                return 0.0
        min_vals.append(_f('min_val'))
        avg_vals.append(_f('avg_val'))
        max_vals.append(_f('max_val'))

    x      = range(len(cols))
    width  = 0.25
    fig, ax = plt.subplots(figsize=(11, 4.5))

    ax.bar([i - width for i in x], min_vals, width, label='MIN',
           color=MP_CYAN, alpha=0.85, edgecolor='white')
    ax.bar(list(x),               avg_vals, width, label='AVG',
           color=MP_BLUE, alpha=0.85, edgecolor='white')
    ax.bar([i + width for i in x], max_vals, width, label='MAX',
           color=MP_DARK, alpha=0.85, edgecolor='white')

    ax.set_xticks(list(x))
    ax.set_xticklabels([c[:20] for c in cols], rotation=15, ha='right', fontsize=9)
    ax.legend(fontsize=9, frameon=False)
    ax.set_title('Numeric Column Statistics (Min / Avg / Max)',
                 fontsize=12, fontweight='bold', color=MP_DARK, pad=10)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    fig.patch.set_facecolor('white')
    fig.tight_layout()
    return _fig_to_image(fig, width_cm=17, height_cm=6)


def _safe_div(num, den):
    return (num / den) if den else 0.0


def _build_confusion_data(test_cases):
    """
    Build confusion matrix for ALL executed test cases.

    Positive class = test expects data to be GOOD (any expected_type except ERROR).
    Negative class = test explicitly expects a query ERROR condition.

    Matrix meaning:
      TP  – expected good, data confirmed good       (PASSED, non-ERROR test)
      FP  – expected good, data has an issue         (FAILED, non-ERROR test)
      TN  – expected error condition, caught it      (FAILED, ERROR test  → error occurred = PASSED logic reversed)
      FN  – expected error condition, missed it      (PASSED, ERROR test  → no error when one was expected)

    Accuracy = (TP + TN) / total  ←  overall "did the test outcome match expectation?"
    """
    tp = fp = tn = fn = 0
    evaluated = 0

    for t in test_cases:
        expected_type = str(
            t.get('expected_type', t.get('expected_result', ''))
        ).strip().upper()
        status = str(t.get('status', '')).strip().upper()

        if status not in ('PASSED', 'FAILED'):
            continue  # skip NOT_RUN

        evaluated += 1

        # Tests of type ERROR predict a failure condition (negative class)
        predicts_good = (expected_type != 'ERROR')
        actually_passed = (status == 'PASSED')

        if predicts_good and actually_passed:
            tp += 1   # correctly confirmed good data
        elif predicts_good and not actually_passed:
            fp += 1   # expected good, found a data issue
        elif not predicts_good and actually_passed:
            fn += 1   # expected an error but query succeeded (missed bad pattern)
        else:
            tn += 1   # correctly caught expected error condition

    total = tp + fp + fn + tn
    accuracy  = _safe_div(tp + tn, total)
    precision = _safe_div(tp, tp + fp)
    recall    = _safe_div(tp, tp + fn)
    f1        = _safe_div(2 * precision * recall, precision + recall)

    return {
        'tp': tp, 'fp': fp, 'tn': tn, 'fn': fn,
        'total': total, 'evaluated': evaluated,
        'accuracy': accuracy, 'precision': precision,
        'recall': recall, 'f1': f1,
    }


def _confusion_matrix_chart(cd):
    """Render a 2x2 confusion matrix heatmap for all executed test cases."""
    # Row 0 = "Expected GOOD data" (non-ERROR tests), Row 1 = "Expected ERROR"
    # Col 0 = Actual PASSED,                          Col 1 = Actual FAILED
    matrix = [
        [cd['tp'], cd['fp']],
        [cd['fn'], cd['tn']],
    ]

    cell_labels = [
        [f'TP\n{cd["tp"]}\nCorrectly confirmed\ngood data',
         f'FP\n{cd["fp"]}\nExpected good,\nfound issue'],
        [f'FN\n{cd["fn"]}\nExpected error,\nquery succeeded',
         f'TN\n{cd["tn"]}\nCorrectly caught\nerror condition'],
    ]

    vmax = max(1, max(max(row) for row in matrix))
    fig, ax = plt.subplots(figsize=(7.5, 5.8))
    im = ax.imshow(matrix, cmap='Blues', vmin=0, vmax=vmax)

    ax.set_xticks([0, 1])
    ax.set_yticks([0, 1])
    ax.set_xticklabels(['Actual: PASSED', 'Actual: FAILED'], fontsize=10, fontweight='bold')
    ax.set_yticklabels(['Expected: GOOD DATA\n(non-ERROR tests)',
                        'Expected: ERROR\n(ERROR tests)'], fontsize=9)
    ax.set_xlabel('Test Execution Outcome', fontsize=10)
    ax.set_ylabel('Test Expectation Type', fontsize=10)
    ax.set_title('Test Suite Confusion Matrix', fontsize=13,
                 fontweight='bold', color=MP_DARK, pad=14)

    for i in range(2):
        for j in range(2):
            value = matrix[i][j]
            txt_color = 'white' if value > (vmax * 0.55) else '#0D1B2A'
            ax.text(j, i, cell_labels[i][j],
                    ha='center', va='center', color=txt_color,
                    fontsize=8.5, fontweight='bold', linespacing=1.4)

    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.ax.tick_params(labelsize=8)
    cbar.set_label('Count', fontsize=8)
    fig.patch.set_facecolor('white')
    fig.tight_layout()
    return _fig_to_image(fig, width_cm=14, height_cm=10)


# ── Page template (header / footer on every page) ────────────────────────────

class _ReportCanvas(pdfcanvas.Canvas):
    def __init__(self, *args, meta=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._meta = meta or {}
        self._pages = []

    def showPage(self):
        self._pages.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        total = len(self._pages)
        for i, page in enumerate(self._pages):
            self.__dict__.update(page)
            self._draw_chrome(i + 1, total)
            super().showPage()
        super().save()

    def _draw_chrome(self, page_num, total):
        if page_num == 1:
            return  # Cover page — no chrome

        W, H = A4
        # Top bar
        self.setFillColor(SNOW_DARK)
        self.rect(0, H - 1.1 * cm, W, 1.1 * cm, fill=1, stroke=0)
        self.setFont('Helvetica-Bold', 9)
        self.setFillColor(WHITE)
        obj  = self._meta.get('full_name', 'Object')
        self.drawString(1 * cm, H - 0.75 * cm, f'Data Quality Report  ·  {obj}')
        self.setFont('Helvetica', 9)
        date = self._meta.get('report_date', '')
        self.drawRightString(W - 1 * cm, H - 0.75 * cm, date)

        # Bottom bar
        self.setFillColor(SNOW_LIGHT)
        self.rect(0, 0, W, 0.9 * cm, fill=1, stroke=0)
        self.setFont('Helvetica', 8)
        self.setFillColor(TEXT_MID)
        self.drawCentredString(W / 2, 0.32 * cm,
            f'CONFIDENTIAL  –  Data Quality Assessment  –  Page {page_num} of {total}')


# ── Main builder ──────────────────────────────────────────────────────────────

def generate_pdf_report(
    object_name: str,
    object_type: str,
    database: str,
    schema: str,
    test_cases: list,
    metadata: dict,
    output_path: str
) -> str:
    """
    Build a professional PDF data quality report.

    Parameters
    ----------
    object_name  : Snowflake object name
    object_type  : TABLE | VIEW | PROCEDURE | FUNCTION
    database     : Database name
    schema       : Schema name
    test_cases   : List of test case dicts (with optional 'status' key)
    metadata     : Object metadata dict (columns, statistics, distinct_values, etc.)
    output_path  : Absolute file path to write the PDF

    Returns
    -------
    output_path  : The path where the PDF was written
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    full_name   = f'{database}.{schema}.{object_name}'
    report_date = datetime.now().strftime('%B %d, %Y  %H:%M')
    st          = _styles()
    W, H        = A4

    # ── KPI calculations ────────────────────────────────────────────────────
    passed   = sum(1 for t in test_cases if str(t.get('status', '')).upper() == 'PASSED')
    failed   = sum(1 for t in test_cases if str(t.get('status', '')).upper() == 'FAILED')
    not_run  = len(test_cases) - passed - failed
    total    = len(test_cases)
    score    = (passed / total * 100) if total > 0 else 0.0

    columns      = metadata.get('columns', [])
    total_rows   = metadata.get('total_rows')
    statistics   = metadata.get('statistics', {})
    dist_values  = metadata.get('distinct_values', {})

    meta = {'full_name': full_name, 'report_date': report_date}

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=1.8 * cm, rightMargin=1.8 * cm,
        topMargin=2.2 * cm, bottomMargin=1.8 * cm,
    )

    story = []

    # ── COVER PAGE ───────────────────────────────────────────────────────────
    story.append(Spacer(1, 1.2 * cm))

    cover_top = Drawing(doc.width, 1.2 * cm)
    cover_top.add(Rect(0, 0, doc.width, 1.2 * cm, fillColor=SNOW_DARK, strokeColor=None))
    cover_top.add(Rect(0, 0, doc.width, 0.12 * cm, fillColor=SNOW_CYAN, strokeColor=None))
    story.append(cover_top)
    story.append(Spacer(1, 1.8 * cm))

    score_color_hex = '#27AE60' if score >= 80 else ('#F39C12' if score >= 50 else '#E74C3C')
    score_style = ParagraphStyle(
        'score_big', fontName='Helvetica-Bold', fontSize=52,
        textColor=colors.HexColor(score_color_hex), leading=56, alignment=TA_CENTER
    )

    cover_rows = [
        [Paragraph('Data Quality', st['cover_title'])],
        [Paragraph('Assessment Report', st['cover_title'])],
        [Paragraph(f'<b>{object_type}</b>  ·  {full_name}', st['cover_sub'])],
        [Spacer(1, 0.2 * cm)],
        [Paragraph(f'<b>{score:.0f}%</b>', score_style)],
        [Paragraph('Overall Health Score', st['cover_meta'])],
        [Paragraph(f'{passed} Passed  ·  {failed} Failed  ·  {not_run} Not Run  ·  {total} Total', st['cover_meta'])],
        [Spacer(1, 0.3 * cm)],
        [Paragraph(f'Generated: {report_date}', st['cover_meta'])],
        [Paragraph('Snowflake Data Quality Tool', st['cover_meta'])],
    ]

    cover_table = Table(cover_rows, colWidths=[doc.width])
    cover_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), SNOW_DARK),
        ('BOX', (0, 0), (-1, -1), 0, SNOW_DARK),
        ('LEFTPADDING', (0, 0), (-1, -1), 20),
        ('RIGHTPADDING', (0, 0), (-1, -1), 20),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]))
    story.append(cover_table)
    story.append(Spacer(1, 1.5 * cm))

    cover_footer = Table([
        [Paragraph('Governance Summary', ParagraphStyle('cover_footer_h', fontName='Helvetica-Bold', fontSize=12, textColor=SNOW_BLUE, alignment=TA_CENTER))],
        [Paragraph('This report summarises data quality health, test execution outcomes, data profiling insights, and governance recommendations for the selected Snowflake object.', ParagraphStyle('cover_footer_b', fontName='Helvetica', fontSize=10, textColor=TEXT_MID, leading=14, alignment=TA_CENTER))],
    ], colWidths=[doc.width])
    cover_footer.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), SNOW_LIGHT),
        ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#D5E8F3')),
        ('LEFTPADDING', (0, 0), (-1, -1), 18),
        ('RIGHTPADDING', (0, 0), (-1, -1), 18),
        ('TOPPADDING', (0, 0), (-1, -1), 12),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
    ]))
    story.append(cover_footer)

    story.append(PageBreak())

    # ── SECTION 1: EXECUTIVE SUMMARY ─────────────────────────────────────────
    story.append(Paragraph('1. Executive Summary', st['section_h']))
    story.append(HRFlowable(width='100%', thickness=1.5,
                             color=SNOW_CYAN, spaceAfter=8))

    # KPI cards as a table
    def kpi_cell(value, label, color_hex='#1E6FA5'):
        return [
            Paragraph(f'<font color="{color_hex}"><b>{value}</b></font>',
                      ParagraphStyle('kv', fontName='Helvetica-Bold', fontSize=26,
                                     leading=30, alignment=TA_CENTER)),
            Paragraph(label,
                      ParagraphStyle('kl', fontName='Helvetica', fontSize=9,
                                     textColor=TEXT_MID, leading=11, alignment=TA_CENTER))
        ]

    score_hex = '#27AE60' if score >= 80 else ('#F39C12' if score >= 50 else '#E74C3C')
    kpi_data = [[
        kpi_cell(f'{score:.0f}%', 'Health Score', score_hex),
        kpi_cell(str(total), 'Total Tests', '#1E6FA5'),
        kpi_cell(str(passed), 'Passed', '#27AE60'),
        kpi_cell(str(failed), 'Failed', '#E74C3C'),
        kpi_cell(str(not_run), 'Not Run', '#95A5A6'),
    ]]

    kpi_table = Table(kpi_data, colWidths=[3.5 * cm] * 5)
    kpi_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), SNOW_LIGHT),
        ('BOX', (0, 0), (-1, -1), 1, SNOW_CYAN),
        ('INNERGRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5E8F3')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ROWPADDING', (0, 0), (-1, -1), 12),
    ]))
    story.append(kpi_table)
    story.append(Spacer(1, 0.5 * cm))

    # Object profile summary
    profile_rows = [
        [Paragraph('<b>Property</b>', st['body_small']),
         Paragraph('<b>Value</b>', st['body_small'])],
        ['Object Name',   object_name],
        ['Object Type',   object_type],
        ['Database',      database],
        ['Schema',        schema],
        ['Fully Qualified', full_name],
        ['Total Rows',    f'{total_rows:,}' if isinstance(total_rows, (int, float)) and total_rows else str(total_rows or 'N/A')],
        ['Column Count',  str(len(columns))],
        ['Report Date',   report_date],
    ]
    prof_table = Table(profile_rows, colWidths=[5 * cm, 11.6 * cm])
    prof_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), SNOW_BLUE),
        ('TEXTCOLOR',  (0, 0), (-1, 0), WHITE),
        ('FONTNAME',   (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE',   (0, 0), (-1, -1), 9),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [WHITE, ROW_ALT]),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#D5E8F3')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ROWPADDING', (0, 0), (-1, -1), 6),
    ]))
    story.append(prof_table)

    story.append(PageBreak())

    # ── SECTION 2: TEST RESULTS VISUALISATION ────────────────────────────────
    story.append(Paragraph('2. Test Results at a Glance', st['section_h']))
    story.append(HRFlowable(width='100%', thickness=1.5,
                             color=SNOW_CYAN, spaceAfter=8))

    # Side by side: donut + bar
    donut_img = _donut_chart(passed, failed, not_run, score)
    story.append(donut_img)
    story.append(Spacer(1, 0.4 * cm))

    if test_cases:
        bar_img = _bar_chart_tests(test_cases)
        story.append(bar_img)

    confusion_data = _build_confusion_data(test_cases)
    run_count = confusion_data['evaluated']
    if run_count > 0:
        story.append(PageBreak())

        story.append(Paragraph('3. Test Suite Accuracy (Confusion Matrix)', st['section_h']))
        story.append(HRFlowable(width='100%', thickness=1.5,
                                 color=SNOW_CYAN, spaceAfter=8))
        story.append(Paragraph(
            'Each test case is treated as a binary classifier: it either expects data to be '  
            '<b>GOOD</b> (any expected type except ERROR) or expects a known '  
            '<b>ERROR condition</b>. The confusion matrix compares those expectations against '  
            'actual execution outcomes to measure how accurately the test suite reflects '  
            'real data quality.',
            st['body']
        ))
        story.append(Spacer(1, 0.3 * cm))

        # ── Prominent accuracy callout ────────────────────────────────────────
        acc_pct  = confusion_data['accuracy'] * 100
        acc_hex  = '#27AE60' if acc_pct >= 80 else ('#F39C12' if acc_pct >= 50 else '#E74C3C')
        acc_style = ParagraphStyle('acc_big', fontName='Helvetica-Bold', fontSize=46,
                                   textColor=colors.HexColor(acc_hex),
                                   leading=50, alignment=TA_CENTER)
        acc_lbl_style = ParagraphStyle('acc_lbl', fontName='Helvetica', fontSize=11,
                                       textColor=TEXT_MID, leading=14, alignment=TA_CENTER)
        acc_callout = Table(
            [
                [Paragraph(f'{acc_pct:.1f}%', acc_style)],
                [Paragraph('Test Suite Accuracy', acc_lbl_style)],
                [Paragraph(
                    f'{confusion_data["tp"] + confusion_data["tn"]} of {run_count} '
                    'tests behaved exactly as expected',
                    ParagraphStyle('acc_sub', fontName='Helvetica', fontSize=9,
                                   textColor=TEXT_MID, leading=12, alignment=TA_CENTER)
                )],
            ],
            colWidths=[doc.width]
        )
        acc_callout.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), SNOW_LIGHT),
            ('BOX', (0, 0), (-1, -1), 1.5, colors.HexColor('#D5E8F3')),
            ('TOPPADDING',    (0, 0), (-1, -1), 14),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 14),
        ]))
        story.append(acc_callout)
        story.append(Spacer(1, 0.4 * cm))

        # ── Matrix heatmap ───────────────────────────────────────────────────
        cm_img = _confusion_matrix_chart(confusion_data)
        story.append(cm_img)
        story.append(Spacer(1, 0.25 * cm))

        # ── Metrics table ────────────────────────────────────────────────────
        acc_color  = colors.HexColor(acc_hex)
        prec_pct   = confusion_data['precision'] * 100
        rec_pct    = confusion_data['recall']    * 100
        f1_pct     = confusion_data['f1']        * 100

        metric_rows = [
            [Paragraph('<b>Metric</b>',      st['body_small']),
             Paragraph('<b>Value</b>',       st['body_small']),
             Paragraph('<b>Meaning in this context</b>', st['body_small'])],
            ['Accuracy',
             Paragraph(f'<font color="{acc_hex}"><b>{acc_pct:.1f}%</b></font>',
                       ParagraphStyle('mv', fontName='Helvetica-Bold', fontSize=9)),
             'Tests where outcome matched expectation'],
            ['Precision',
             f'{prec_pct:.1f}%',
             'Of "good data" tests, % that actually passed'],
            ['Recall',
             f'{rec_pct:.1f}%',
             'Of truly passing scenarios, % correctly expected to pass'],
            ['F1 Score',
             f'{f1_pct:.1f}%',
             'Harmonic mean of Precision & Recall'],
            ['True Positive (TP)',  str(confusion_data['tp']),
             'Expected good data → confirmed good (PASSED)'],
            ['False Positive (FP)', str(confusion_data['fp']),
             'Expected good data → found an issue (FAILED)'],
            ['True Negative (TN)',  str(confusion_data['tn']),
             'Expected error condition → correctly caught (FAILED)'],
            ['False Negative (FN)', str(confusion_data['fn']),
             'Expected error condition → query unexpectedly succeeded (PASSED)'],
            ['Tests Evaluated', str(run_count), 'Excludes NOT_RUN tests'],
        ]
        metrics_table = Table(metric_rows, colWidths=[4 * cm, 3 * cm, 9.6 * cm])
        metrics_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), SNOW_BLUE),
            ('TEXTCOLOR',  (0, 0), (-1, 0), WHITE),
            ('FONTNAME',   (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0, 0), (-1, -1), 8.5),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [WHITE, ROW_ALT]),
            ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#D5E8F3')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ROWPADDING', (0, 0), (-1, -1), 5),
            ('FONTNAME', (0, 1), (0, 4), 'Helvetica-Bold'),
        ]))
        story.append(metrics_table)

    story.append(PageBreak())

    # ── SECTION 4: DATA PROFILE ───────────────────────────────────────────────
    story.append(Paragraph('4. Data Profile', st['section_h']))
    story.append(HRFlowable(width='100%', thickness=1.5,
                             color=SNOW_CYAN, spaceAfter=8))

    if columns:
        story.append(Paragraph('Column Inventory', st['sub_h']))
        col_rows = [[
            Paragraph('<b>#</b>', st['body_small']),
            Paragraph('<b>Column Name</b>', st['body_small']),
            Paragraph('<b>Data Type</b>', st['body_small']),
            Paragraph('<b>Nullable</b>', st['body_small']),
        ]]
        for i, col in enumerate(columns):
            c_name  = col.get('name', col.get('column_name', 'N/A'))
            c_type  = col.get('type', col.get('data_type', 'N/A'))
            c_null  = col.get('null?', col.get('nullable', col.get('is_nullable', 'Y')))
            row_bg  = WHITE if i % 2 == 0 else ROW_ALT
            col_rows.append([str(i + 1), c_name, c_type, str(c_null)])
        col_table = Table(col_rows, colWidths=[1.2 * cm, 6.5 * cm, 5 * cm, 3.9 * cm])
        col_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), SNOW_BLUE),
            ('TEXTCOLOR',  (0, 0), (-1, 0), WHITE),
            ('FONTNAME',   (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0, 0), (-1, -1), 8),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [WHITE, ROW_ALT]),
            ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#D5E8F3')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ROWPADDING', (0, 0), (-1, -1), 5),
        ]))
        story.append(col_table)
        story.append(Spacer(1, 0.5 * cm))

    # Numeric statistics chart
    if statistics:
        story.append(Paragraph('Numeric Column Statistics', st['sub_h']))
        stats_img = _stats_bar_chart(statistics)
        if stats_img:
            story.append(stats_img)

        stats_rows = [[
            Paragraph('<b>Column</b>', st['body_small']),
            Paragraph('<b>MIN</b>', st['body_small']),
            Paragraph('<b>AVG</b>', st['body_small']),
            Paragraph('<b>MAX</b>', st['body_small']),
        ]]
        for col_name, s in list(statistics.items())[:10]:
            def _fmt(k):
                v = s.get(k) or s.get(k.lower()) or s.get(k.upper())
                try:
                    return f'{float(v):,.2f}' if v is not None else 'N/A'
                except (TypeError, ValueError):
                    return str(v) if v is not None else 'N/A'
            stats_rows.append([col_name, _fmt('min_val'), _fmt('avg_val'), _fmt('max_val')])

        stats_table = Table(stats_rows, colWidths=[5 * cm, 4.5 * cm, 4.5 * cm, 4.6 * cm])
        stats_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), SNOW_BLUE),
            ('TEXTCOLOR',  (0, 0), (-1, 0), WHITE),
            ('FONTNAME',   (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0, 0), (-1, -1), 9),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [WHITE, ROW_ALT]),
            ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#D5E8F3')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ROWPADDING', (0, 0), (-1, -1), 5),
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
        ]))
        story.append(stats_table)
        story.append(Spacer(1, 0.5 * cm))

    # Distinct / categorical values
    if dist_values:
        story.append(Paragraph('Categorical Column Value Distribution', st['sub_h']))
        for col_name, info in list(dist_values.items())[:8]:
            if not isinstance(info, dict):
                continue
            count  = info.get('count', '?')
            vals   = info.get('values', [])
            source = info.get('source', 'view')
            src_obj = info.get('source_object', '')
            src_note = f' (sourced from {src_obj})' if source == 'base_table' and src_obj else ''

            story.append(Paragraph(
                f'<b>{col_name}</b>  —  {count} distinct value(s){src_note}',
                st['body']))

            if isinstance(vals, list) and vals:
                chips  = ', '.join([f'<font color="#1E6FA5">{v}</font>' for v in vals[:60]])
                story.append(Paragraph(chips, st['body_small']))
            elif isinstance(vals, str):
                story.append(Paragraph(vals, st['body_small']))
            story.append(Spacer(1, 0.2 * cm))

    story.append(PageBreak())

    # ── SECTION 5: DETAILED TEST RESULTS ─────────────────────────────────────
    story.append(Paragraph('5. Detailed Test Results', st['section_h']))
    story.append(HRFlowable(width='100%', thickness=1.5,
                             color=SNOW_CYAN, spaceAfter=8))

    for i, t in enumerate(test_cases):
        status_raw = str(t.get('status', 'NOT RUN')).upper()
        if status_raw == 'PASSED':
            badge_color = PASS_GREEN
            badge_label = '✓  PASSED'
            style_key   = 'pass'
        elif status_raw == 'FAILED':
            badge_color = FAIL_RED
            badge_label = '✗  FAILED'
            style_key   = 'fail'
        else:
            badge_color = SKIP_GREY
            badge_label = '–  NOT RUN'
            style_key   = 'skip'

        t_name = t.get('test_name', f'Test {i+1}')
        t_desc = t.get('description', '')
        t_qry  = t.get('query', '')
        t_exp  = t.get('expected_type', t.get('expected_result', ''))
        t_msg  = t.get('message', '')

        # Row with coloured left border via a mini-table
        header_row = [[
            Paragraph(f'<b>Test {i+1}:</b>  {t_name}', st['body']),
            Paragraph(f'<b>{badge_label}</b>',
                      ParagraphStyle('badge', fontName='Helvetica-Bold',
                                     fontSize=9, textColor=badge_color,
                                     alignment=TA_RIGHT)),
        ]]
        ht = Table(header_row, colWidths=[13 * cm, 3.6 * cm])
        ht.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), SNOW_LIGHT),
            ('LINEAFTER',  (0, 0), (0,  0), 0, WHITE),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ROWPADDING', (0, 0), (-1, -1), 7),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 7),
        ]))

        detail_rows = []
        if t_desc:
            detail_rows.append(['Description', t_desc])
        if t_exp:
            detail_rows.append(['Expected Type', t_exp])
        if t_msg:
            detail_rows.append(['Result Message', t_msg])
        if t_qry:
            detail_rows.append(['SQL Query', Paragraph(t_qry, st['code'])])

        det_table_data = [['', '']] + (detail_rows if detail_rows else [['', '']])
        # Build properly
        det_rows_built = [[
            Paragraph(f'<b>{d[0]}</b>', st['body_small']) if isinstance(d[0], str) else d[0],
            Paragraph(str(d[1]), st['body_small']) if isinstance(d[1], str) else d[1]
        ] for d in detail_rows] if detail_rows else []

        block = KeepTogether([
            ht,
            *(
                [Table(det_rows_built, colWidths=[3.8 * cm, 12.8 * cm],
                    style=TableStyle([
                        ('FONTSIZE', (0, 0), (-1, -1), 8),
                        ('ROWBACKGROUNDS', (0, 0), (-1, -1), [WHITE, ROW_ALT]),
                        ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#E8EDF2')),
                        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                        ('ROWPADDING', (0, 0), (-1, -1), 5),
                    ]))]
                if det_rows_built else []
            ),
            Spacer(1, 0.3 * cm),
        ])
        story.append(block)

    story.append(PageBreak())

    # ── SECTION 6: RECOMMENDATIONS ───────────────────────────────────────────
    story.append(Paragraph('6. Recommendations', st['section_h']))
    story.append(HRFlowable(width='100%', thickness=1.5,
                             color=SNOW_CYAN, spaceAfter=8))

    recs = _build_recommendations(test_cases, metadata, score)
    for rec in recs:
        story.append(Paragraph(f'• {rec}', st['recommendation']))
        story.append(Spacer(1, 0.15 * cm))

    # ── Build PDF ─────────────────────────────────────────────────────────────
    doc.build(
        story,
        canvasmaker=lambda *a, **kw: _ReportCanvas(*a, meta=meta, **kw),
    )

    return output_path


def _build_recommendations(test_cases, metadata, score):
    """Generate data governance recommendations from test results and metadata."""
    recs = []

    failed_tests  = [t for t in test_cases if str(t.get('status', '')).upper() == 'FAILED']
    not_run_tests = [t for t in test_cases if str(t.get('status', '')).upper() not in ('PASSED', 'FAILED')]

    if score == 100:
        recs.append('All tests passed. Object meets current data quality thresholds. Consider expanding test coverage.')
    elif score >= 80:
        recs.append(f'Health score of {score:.0f}% is Good. Address the {len(failed_tests)} failed test(s) to reach full compliance.')
    elif score >= 50:
        recs.append(f'Health score of {score:.0f}% indicates moderate quality issues. Prioritise fixing failed tests before promoting data to production.')
    else:
        recs.append(f'Health score of {score:.0f}% is Critical. This object should be quarantined until data quality issues are resolved.')

    if failed_tests:
        recs.append(f'Investigate the following failed tests: '
                    + ', '.join([f'"{t.get("test_name", "?")}"' for t in failed_tests[:5]])
                    + ('...' if len(failed_tests) > 5 else '.'))
        recs.append('For each failed test, trace the root cause upstream — check source system feeds, ETL pipeline transforms, and ingestion schedules.')

    if not_run_tests:
        recs.append(f'{len(not_run_tests)} test(s) have not been executed. Run all tests before using this report for governance sign-off.')

    stats = metadata.get('statistics', {})
    for col, s in stats.items():
        mn = s.get('min_val') or s.get('MIN_VAL')
        mx = s.get('max_val') or s.get('MAX_VAL')
        try:
            if float(mn) < 0:
                recs.append(f'Column <b>{col}</b> contains negative values (min = {mn}). Verify if this is expected by the business.')
        except (TypeError, ValueError):
            pass

    columns = metadata.get('columns', [])
    nullable_keys = [col for col in columns
                     if str(col.get('null?', col.get('nullable', 'Y'))).upper() in ('Y', 'YES', 'TRUE', '1')
                     and col.get('name', '').upper() not in ('DESCRIPTION', 'NOTES', 'COMMENTS')]
    if len(nullable_keys) > len(columns) * 0.6 and columns:
        recs.append('More than 60% of columns are nullable. Review whether critical business key columns should be enforced as NOT NULL.')

    dist_values = metadata.get('distinct_values', {})
    for col_name, info in list(dist_values.items())[:5]:
        if isinstance(info, dict):
            count = info.get('count', 0)
            try:
                if int(count) == 1:
                    recs.append(f'Column <b>{col_name}</b> has only 1 distinct value. It may be a constant and could be removed or used as a partition key.')
            except (TypeError, ValueError):
                pass

    total_rows = metadata.get('total_rows')
    try:
        if total_rows is not None and int(total_rows) == 0:
            recs.append('The object has 0 rows. Verify that data loading pipelines are functioning correctly.')
    except (TypeError, ValueError):
        pass

    recs.append('Schedule this report to run automatically after each ETL load for continuous data quality monitoring.')
    recs.append('Version and maintain the generated test cases alongside your pipeline code so they can be reused for automated regression testing.')

    return recs
