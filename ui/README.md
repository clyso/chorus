# Chorus UI

Chorus UI is the user interface for **Chorus**, designed to manage and interact with the storage system. It is built using Vue 3, TypeScript, and Pinia for state management. The UI relies on the [@clyso/clyso-ui-kit](https://www.npmjs.com/package/@clyso/clyso-ui-kit), which is based on the Naive UI library, providing a set of pre-built components for the application.

## Features

- **Storages** — View and manage storage backends, set credentials
- **Replication** — Configure and monitor data replication between storages
- **Diff Reports** — Compare bucket contents across storages and inspect inconsistencies
- **Routing Policies** — Define how requests are routed to storage backends
- **Monitoring** — Dashboards with Prometheus-backed metrics (under construction)
- **i18n** — Multi-language support (English, German)

## Prerequisites

- [Node.js](https://nodejs.org/) 20+
- [Yarn](https://classic.yarnpkg.com/) 1.x (`yarn@1.22.22` pinned via `packageManager`)
- A running Chorus backend (default: `http://localhost:9671`)

## Getting Started

```bash
# Install dependencies
yarn install

# Start dev server (http://localhost:8081 by default)
yarn dev
```

The Vite dev server proxies `/api` requests to `http://localhost:9671`. See [vite.config.ts](vite.config.ts) for proxy details.

## Scripts

| Command | Description |
|---|---|
| `yarn dev` | Start Vite dev server |
| `yarn build` | Production build to `build/` |
| `yarn preview` | Preview production build locally |
| `yarn lint` | Run all linters (types, eslint, oxlint, stylelint, prettier) |
| `yarn fix` | Auto-fix all linters |

## Project Structure

```
src/
├── assets/          # Icons, images, static resources
├── components/      # Reusable Vue components
├── http/            # Axios HTTP client setup
├── i18n/            # Translations (en, de)
├── pages/           # Route-level page components
├── router/          # Vue Router config and guards
├── services/        # API service layers (Chorus, Prometheus)
├── stores/          # Pinia stores (per feature + forms)
├── styles/          # Global SCSS
└── utils/           # Helpers, types, composables, constants
```

## Tech Stack

- **Framework:** [Vue 3](https://vuejs.org/) (Composition API)
- **Language:** [TypeScript](https://www.typescriptlang.org/)
- **State:** [Pinia](https://pinia.vuejs.org/)
- **Routing:** [Vue Router](https://router.vuejs.org/)
- **UI Kit:** [@clyso/clyso-ui-kit](https://www.npmjs.com/package/@clyso/clyso-ui-kit) (based on [Naive UI](https://www.naiveui.com/))
- **Build:** [Vite](https://vite.dev/)
- **Charts:** [Chart.js](https://www.chartjs.org/)
- **Validation:** [Vuelidate](https://vuelidate-next.netlify.app/)
- **i18n:** [Vue I18n](https://vue-i18n.intlify.dev/)


## License

[AGPL-3.0](LICENSE) — Clyso GmbH
