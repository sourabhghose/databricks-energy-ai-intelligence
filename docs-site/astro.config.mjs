import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://sourabhghose.github.io',
  base: '/databricks-energy-ai-intelligence',
  integrations: [
    starlight({
      title: 'Databricks Energy AI Intelligence',
      description: 'AI-powered market intelligence for the Australian National Electricity Market — built on Databricks',
      logo: { src: './src/assets/logo.svg' },
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/sourabhghose/databricks-energy-copilot' },
      ],
      customCss: ['./src/styles/custom.css'],
      sidebar: [
        {
          label: 'Getting Started',
          items: [
            { label: 'Overview', slug: 'getting-started/overview' },
            { label: 'Architecture', slug: 'getting-started/architecture' },
            { label: 'Quick Start', slug: 'getting-started/quickstart' },
            { label: 'Deployment Guide', slug: 'getting-started/deployment' },
          ],
        },
        {
          label: 'Front Office',
          items: [
            { label: 'Overview', slug: 'front-office/overview' },
            { label: 'Live Market & NEM Prices', slug: 'front-office/live-market' },
            { label: 'Generation & Fuel Mix', slug: 'front-office/generation' },
            { label: 'Renewables', slug: 'front-office/renewables' },
            { label: 'Energy Storage & Battery', slug: 'front-office/storage' },
            { label: 'FCAS & Ancillary Services', slug: 'front-office/fcas' },
            { label: 'Gas Market', slug: 'front-office/gas' },
            { label: 'Demand & Weather', slug: 'front-office/demand-weather' },
            { label: 'NEM Market Briefs', slug: 'front-office/market-briefs' },
          ],
        },
        {
          label: 'Middle Office',
          items: [
            { label: 'Overview', slug: 'middle-office/overview' },
            { label: 'Deal Capture & ETRM', slug: 'middle-office/deal-capture' },
            { label: 'Portfolio & P&L', slug: 'middle-office/portfolio' },
            { label: 'Forward Curves', slug: 'middle-office/forward-curves' },
            { label: 'Risk Management', slug: 'middle-office/risk' },
            { label: 'Trading Signals', slug: 'middle-office/trading-signals' },
            { label: 'Bidding Strategy', slug: 'middle-office/bidding' },
            { label: 'Battery Optimisation', slug: 'middle-office/battery-optimisation' },
          ],
        },
        {
          label: 'Back Office',
          items: [
            { label: 'Overview', slug: 'back-office/overview' },
            { label: 'Settlement', slug: 'back-office/settlement' },
            { label: 'Compliance & Regulatory', slug: 'back-office/compliance' },
            { label: 'Environmental & LGCs', slug: 'back-office/environmentals' },
            { label: 'Network Operations', slug: 'back-office/network-ops' },
            { label: 'Reporting', slug: 'back-office/reporting' },
          ],
        },
        {
          label: 'DNSP Intelligence',
          items: [
            { label: 'Overview', slug: 'dnsp/overview' },
            { label: 'AER Regulatory', slug: 'dnsp/aer-regulatory' },
            { label: 'AIO & STPIS Compliance', slug: 'dnsp/aio-stpis' },
            { label: 'Asset Intelligence', slug: 'dnsp/asset-intelligence' },
            { label: 'RAB Roll-Forward', slug: 'dnsp/rab' },
            { label: 'Vegetation Risk', slug: 'dnsp/vegetation-risk' },
            { label: 'Hosting Capacity & DER', slug: 'dnsp/hosting-capacity' },
            { label: 'Workforce Analytics', slug: 'dnsp/workforce' },
            { label: 'Benchmarking', slug: 'dnsp/benchmarking' },
            { label: 'DAPR Assembly', slug: 'dnsp/dapr' },
          ],
        },
        {
          label: 'AI & ML Capabilities',
          items: [
            { label: 'Overview', slug: 'ai-ml/overview' },
            { label: 'AI Market Intelligence (Copilot)', slug: 'ai-ml/copilot' },
            { label: 'Genie AI/BI Spaces', slug: 'ai-ml/genie' },
            { label: 'Asset Failure Prediction', slug: 'ai-ml/asset-failure' },
            { label: 'AIO Draft Generator (Claude)', slug: 'ai-ml/aio-draft' },
            { label: 'Vegetation Risk ML', slug: 'ai-ml/vegetation-ml' },
            { label: 'Workforce Demand Forecasting', slug: 'ai-ml/workforce-forecast' },
            { label: 'STPIS Anomaly Detection', slug: 'ai-ml/stpis-anomaly' },
            { label: 'NEM Price Forecasting', slug: 'ai-ml/price-forecast' },
          ],
        },
        {
          label: 'Data Platform',
          items: [
            { label: 'Architecture Overview', slug: 'data/architecture' },
            { label: 'Data Sources', slug: 'data/sources' },
            { label: 'Pipeline Jobs', slug: 'data/pipelines' },
            { label: 'Gold Tables Reference', slug: 'data/gold-tables' },
            { label: 'Real-Time NEM Data', slug: 'data/real-time' },
          ],
        },
        {
          label: 'API Reference',
          items: [
            { label: 'Overview', slug: 'api/overview' },
            { label: 'Market Data APIs', slug: 'api/market-data' },
            { label: 'DNSP APIs', slug: 'api/dnsp' },
            { label: 'AI & ML APIs', slug: 'api/ai-ml' },
            { label: 'Genie API', slug: 'api/genie' },
          ],
        },
      ],
    }),
  ],
});
