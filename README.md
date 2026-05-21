#Strava Pro Dashboard & Data Pipeline

![Next.js](https://img.shields.io/badge/Next.js-16-black?logo=next.js)
![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python)
![PostgreSQL](https://img.shields.io/badge/Neon_DB-Serverless-336791?logo=postgresql)
![Tailwind CSS](https://img.shields.io/badge/Tailwind_CSS-v4-38B2AC?logo=tailwind-css)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-Automated-2088FF?logo=github-actions)

A professional Full-stack Monorepo designed to automatically extract, transform, store, and visualize advanced running performance metrics from the Strava API. This platform processes time-series data to deliver deep athletic insights—such as Heart Rate Zones (HR Zones) distribution, Cadence tracking, and automated Splits/Km analysis—bypassing the limitations of the standard Strava free tier.

##Key Features
- **Automated ETL Data Pipeline:** Robust Python script handling OAuth2 authentication, rate limiting, and time-series stream data extraction from Strava API.
- **DevOps & Automation:** Configured GitHub Actions Workflows (Cron Jobs) to seamlessly trigger the ETL pipeline daily without manual intervention.
- **Modern Full-Stack Architecture:** Next.js 16 (App Router) utilizing Server/Client Components for lightning-fast performance and optimal state management.
- **Interactive Data Visualization:** Dynamic UI layouts featuring responsive charts (Composed, Area, Pie Charts) built with Recharts to highlight training loads and performance trends.
- **Secure Secret Management:** Complete protection of sensitive API credentials and Neon DB connection strings leveraging GitHub Secrets.

---

##Project Structure (Monorepo)

```text
strava-data-pipeline/
├── .github/workflows/   # CI/CD Automation Workflow (Daily Sync Cron Job)
├── app.py               # Core Python ETL script for Strava API integration
├── requirements.txt     # Python dependencies and data packages
├── app/                 # Next.js Application Pages & Serverless API Routes
├── components/          # Reusable UI components & Recharts instances
├── public/              # Static assets, icons, and branding resources
├── package.json         # Node.js project manifests & dependencies
└── next.config.ts       # Next.js framework configuration

```
##How to run DashBoard
# Clean up cache and previous build traces
rm -rf node_modules package-lock.json .next
npm cache clean --force

# Install dependencies and start server
npm install
npm run dev
