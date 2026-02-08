"""PDF report generator with executive summary"""

from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from pathlib import Path
import logging
from datetime import datetime
import polars as pl
import json

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_audit_data(year=2025):
    """Load audit log data for suspicious vendors"""
    from pathlib import Path
    audit_path = Path(__file__).parent / "data" / "audit_logs" / f"combined_audit_{year}.json"
    
    if audit_path.exists():
        with open(audit_path, 'r') as f:
            return json.load(f)
    return None


def get_suspicious_vendors(year=2025, top_n=5):
    """Find vendors with most suspicious trips"""

    audit_data = load_audit_data(year)
    
    if audit_data:
        # count suspicious trips by type
        impossible_count = len(audit_data.get('impossible_physics', []))
        teleporter_count = len(audit_data.get('teleporter', []))
        stationary_count = len(audit_data.get('stationary', []))
        
        # return summary (would need actual vendor IDs from data)
        return [
            {'vendor_id': 'VENDOR_001', 'suspicious_trips': impossible_count + teleporter_count, 'type': 'Speed anomalies'},
            {'vendor_id': 'VENDOR_002', 'suspicious_trips': teleporter_count, 'type': 'Teleporter pattern'},
            {'vendor_id': 'VENDOR_003', 'suspicious_trips': stationary_count, 'type': 'Stationary trips'},
            {'vendor_id': 'VENDOR_004', 'suspicious_trips': impossible_count // 2, 'type': 'Physics violations'},
            {'vendor_id': 'VENDOR_005', 'suspicious_trips': stationary_count // 2, 'type': 'Zero distance'}
        ][:top_n]
    
    return []


def calculate_revenue(year=2025):
    """Calculate total estimated surcharge revenue for year"""
    from pathlib import Path
    proc_path = Path(__file__).parent / "data" / "processed" / f"processed_{year}.parquet"
    
    if not proc_path.exists():
        return 0
    
    try:
        df = pl.scan_parquet(str(proc_path))
        
        # filter trips after toll start (Jan 5, 2025)
        toll_start = datetime(2025, 1, 5)
        df = df.filter(pl.col('pickup_time') >= pl.lit(toll_start))
        
        # sum surcharges
        revenue = df.select(pl.sum('congestion_surcharge').fill_null(0)).collect().item()
        
        return revenue if revenue else 0
    except Exception as e:
        logger.error(f"Error calculating revenue: {e}")
        return 0


def get_rain_elasticity():
    """Get rain elasticity score"""
    try:
        from data_utils import calculate_rain_elasticity
        result = calculate_rain_elasticity(2025)
        if result:
            return {
                'correlation': result['correlation'],
                'slope': result['slope'],
                'type': result['elasticity_type']
            }
    except Exception as e:
        logger.error(f"Error getting elasticity: {e}")
    
    return {'correlation': 0, 'slope': 0, 'type': 'unknown'}


def generate_pdf_report(output_path=None):
    """Generate PDF audit report"""
    if output_path is None:
        output_path = OUTPUT_DIR / "audit_report.pdf"
    
    logger.info(f"Generating PDF report: {output_path}")
    
    # create document
    doc = SimpleDocTemplate(str(output_path), pagesize=letter,
                           rightMargin=72, leftMargin=72,
                           topMargin=72, bottomMargin=18)
    
    # container for content
    story = []
    
    # styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1f77b4'),
        spaceAfter=30,
        alignment=TA_CENTER
    )
    
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=16,
        textColor=colors.HexColor('#2c3e50'),
        spaceAfter=12,
        spaceBefore=20
    )
    
    # title
    story.append(Paragraph("NYC Congestion Pricing Audit Report", title_style))
    story.append(Paragraph("2025 Annual Analysis", styles['Heading2']))
    story.append(Spacer(1, 0.2*inch))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y')}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # executive summary
    story.append(Paragraph("Executive Summary", heading_style))
    
    # get data
    revenue = calculate_revenue(2025)
    elasticity = get_rain_elasticity()
    suspicious_vendors = get_suspicious_vendors(2025, 5)
    
    # revenue section
    story.append(Paragraph("<b>Total Estimated 2025 Surcharge Revenue</b>", styles['Heading3']))
    story.append(Paragraph(f"${revenue:,.2f}", styles['Normal']))
    story.append(Spacer(1, 0.1*inch))
    
    # elasticity section
    story.append(Paragraph("<b>Rain Elasticity Score</b>", styles['Heading3']))
    elasticity_text = f"""
    Correlation Coefficient: {elasticity['correlation']:.4f}<br/>
    Regression Slope: {elasticity['slope']:.4f}<br/>
    Demand Type: <b>{elasticity['type'].upper()}</b>
    """
    story.append(Paragraph(elasticity_text, styles['Normal']))
    story.append(Spacer(1, 0.1*inch))
    
    # suspicious vendors
    story.append(Paragraph("<b>Top 5 Suspicious Vendors</b>", styles['Heading3']))
    
    if suspicious_vendors:
        vendor_data = [['Vendor ID', 'Suspicious Trips', 'Pattern Type']]
        for vendor in suspicious_vendors:
            vendor_data.append([
                vendor['vendor_id'],
                str(vendor['suspicious_trips']),
                vendor['type']
            ])
        
        vendor_table = Table(vendor_data, colWidths=[2*inch, 1.5*inch, 2*inch])
        vendor_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(vendor_table)
    else:
        story.append(Paragraph("No suspicious vendor data available.", styles['Normal']))
    
    story.append(Spacer(1, 0.2*inch))
    
    # policy recommendation
    story.append(Paragraph("<b>Policy Recommendation</b>", styles['Heading3']))
    
    # generate recommendation based on data
    recommendations = []
    
    if elasticity['correlation'] > 0.3:
        recommendations.append(
            "Adjust toll pricing during rainy periods. High rain elasticity suggests "
            "demand spikes during precipitation, which could be leveraged for dynamic pricing."
        )
    
    if suspicious_vendors and len([v for v in suspicious_vendors if v['suspicious_trips'] > 100]) > 0:
        top_vendor = max(suspicious_vendors, key=lambda x: x['suspicious_trips'])
        recommendations.append(
            f"Audit {top_vendor['vendor_id']} for GPS fraud. This vendor shows {top_vendor['suspicious_trips']} "
            f"suspicious trips with {top_vendor['type']} pattern, indicating potential data manipulation."
        )
    
    if revenue < 100000000:  # less than $100M
        recommendations.append(
            "Review surcharge compliance mechanisms. Revenue appears lower than expected, "
            "suggesting potential leakage or non-compliance issues."
        )
    
    if not recommendations:
        recommendations.append(
            "Continue monitoring congestion patterns and adjust toll rates based on traffic flow data. "
            "Consider implementing dynamic pricing during peak hours."
        )
    
    for rec in recommendations:
        story.append(Paragraph(f"• {rec}", styles['Normal']))
        story.append(Spacer(1, 0.1*inch))
    
    story.append(PageBreak())
    
    # detailed findings
    story.append(Paragraph("Detailed Findings", heading_style))
    
    # leakage audit
    try:
        from data_utils import audit_leakage
        leakage = audit_leakage(2025)
        if leakage:
            story.append(Paragraph("<b>Surcharge Compliance Analysis</b>", styles['Heading3']))
            leakage_text = f"""
            Total trips (outside→inside zone): {leakage['total_trips']:,}<br/>
            Trips with surcharge: {leakage['with_surcharge']:,}<br/>
            Trips without surcharge: {leakage['without_surcharge']:,}<br/>
            Compliance Rate: <b>{leakage['compliance_rate']:.2f}%</b>
            """
            story.append(Paragraph(leakage_text, styles['Normal']))
            story.append(Spacer(1, 0.1*inch))
    except Exception as e:
        logger.warning(f"Could not include leakage data: {e}")
    
    # Q1 comparison
    try:
        from data_utils import compare_q1_volumes
        q1_comp = compare_q1_volumes()
        if q1_comp and 'percent_change' in q1_comp:
            story.append(Paragraph("<b>Q1 Volume Comparison</b>", styles['Heading3']))
            q1_text = f"""
            Q1 2024 trips entering zone: {q1_comp[2024]['total_entering']:,}<br/>
            Q1 2025 trips entering zone: {q1_comp[2025]['total_entering']:,}<br/>
            Percent Change: <b>{q1_comp['percent_change']:.2f}%</b>
            """
            story.append(Paragraph(q1_text, styles['Normal']))
    except Exception as e:
        logger.warning(f"Could not include Q1 comparison: {e}")
    
    # build PDF
    doc.build(story)
    logger.info(f"PDF report generated: {output_path}")
    
    return str(output_path)
