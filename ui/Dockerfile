# Build stage
FROM node:20 as build
WORKDIR /app

# Install dependencies
COPY package.json yarn.lock ./
RUN yarn install --pure-lockfile

# Copy source files and build
COPY . .
RUN yarn run build

# Production stage
FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
COPY nginx/default.conf /etc/nginx/nginx.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
