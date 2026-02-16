# Lucy Walker Jewelry — Client Portal Demo

**Client:** Lucy Walker Jewelry (luxury jewelry e-commerce)
**Service:** Client Portal — Executive Summary Dashboard
**Data:** Mock Google Ads performance metrics
**Generated:** February 9, 2026

---

## File Structure

```
lucy-walker-jewelry-portal/
├── config.json          # Brand configuration (colors, layout)
├── data.json            # Mock campaign performance data
├── index.html           # Interactive dashboard (open in browser)
├── ONBOARDING.md        # Client-facing walkthrough guide
└── README.md            # This file (technical documentation)
```

---

## Mock Data Summary

### Account Overview

| Metric | Value |
|--------|-------|
| Total Spend | $4,250 |
| Total Clicks | 892 |
| Total Conversions | 11 |
| Total Revenue | $23,400 |
| ROAS | 5.51x |
| Avg Order Value | $2,127 |
| Cost Per Conversion | $386 |

### Campaigns

| Campaign | Type | Spend | Revenue | ROAS |
|----------|------|-------|---------|------|
| Brand - Search | Search | $680 | $9,200 | 13.53x |
| Shopping - Engagement Rings | Shopping | $1,520 | $8,100 | 5.33x |
| Shopping - Wedding Bands | Shopping | $890 | $3,200 | 3.60x |
| Search - Generic Engagement | Search | $780 | $2,100 | 2.69x |
| Remarketing - All Visitors | Display | $380 | $800 | 2.11x |

Data is realistic for luxury jewelry e-commerce:
- Conversion rates: 0.95% – 2.13% (typical for high-AOV products)
- Average order values: $800 – $2,700 (luxury price range)
- Brand search dominates ROAS (high intent, low CPC)

---

## Brand Configuration

| Setting | Value |
|---------|-------|
| Primary Color | `#1B3A52` (Dark Navy) |
| Accent Color | `#D64C00` (Bold Orange) |
| Layout | Bento Grid |
| Font | Space Grotesk (Google Fonts) |

---

## Regenerating the Dashboard

To regenerate the base HTML from config and data:

```bash
node .claude/skills/sellable-client-portal/scripts/generate-dashboard.mjs \
  --config output/lucy-walker-jewelry-portal/config.json \
  --data output/lucy-walker-jewelry-portal/data.json \
  --output output/lucy-walker-jewelry-portal/
```

Note: The current `index.html` has been enhanced beyond the base template with:
- 6 KPI cards (base template generates 4)
- SVG bar chart (base template has a placeholder)
- Campaign performance table with ROAS bars
- Jewelry-specific AI insights
- Dark navy header with navigation
- Date range selector UI
- Responsive breakpoints and print styles

---

## Going Live

### Requirements

1. **Google Ads API access** — OAuth2 credentials for the client's account
2. **Hosting** — Static site host (Vercel, Netlify) or Node.js server
3. **Authentication** — Login system (Auth0, Supabase, or custom)
4. **Scheduling** — Daily data fetch via cron job or serverless function

### Architecture (Production)

```
Google Ads API → Daily fetch script → data.json → Dashboard rebuild → CDN
                                                                      ↓
                                          Client logs in → Sees live dashboard
```

### Customization Options

- Additional dashboards (Campaign Details, Search Terms, Shopping)
- Date range picker with real data filtering
- PDF/CSV export functionality
- Email alert system for KPI threshold changes
- Multi-account support for agencies
