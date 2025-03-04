# Build stage
FROM node:20 as build-stage
WORKDIR /app

# Install dependencies
COPY package.json yarn.lock ./
ENV NODE_ENV=development
RUN yarn install
COPY . .
# Build
ENV NODE_ENV=production
RUN yarn build

# Production stage
FROM nginx:stable-alpine as production-stage
COPY --from=build-stage /app/build /usr/share/nginx/html
COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template

ARG GIT_COMMIT
ARG GIT_TAG
ENV GIT_COMMIT=$GIT_COMMIT
ENV GIT_TAG=$GIT_TAG

# --env BACKEND_URL=http://127.0.0.1:9090
