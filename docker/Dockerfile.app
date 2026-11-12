# Imagen unica para los 6 servicios NestJS.
# El comando que arranca cada servicio se define en docker-compose.yml
# (command: node apps/<servicio>/dist/main.js).
FROM node:22-alpine AS build
WORKDIR /repo

# Primero solo los manifestos: npm ci queda cacheado mientras no cambien.
COPY package.json package-lock.json ./
COPY libs/contracts/package.json libs/contracts/package.json
COPY libs/kafka-toolkit/package.json libs/kafka-toolkit/package.json
COPY apps/activation-api/package.json apps/activation-api/package.json
COPY apps/billing-service/package.json apps/billing-service/package.json
COPY apps/provisioning-service/package.json apps/provisioning-service/package.json
COPY apps/notification-service/package.json apps/notification-service/package.json
COPY apps/crm-analytics-service/package.json apps/crm-analytics-service/package.json
COPY apps/loyalty-service/package.json apps/loyalty-service/package.json
COPY apps/demo-ui/package.json apps/demo-ui/package.json
RUN npm ci --no-audit --no-fund

# Fuentes de libs y servicios (demo-ui se construye en su propia imagen).
COPY tsconfig.base.json ./
COPY libs ./libs
COPY apps/activation-api ./apps/activation-api
COPY apps/billing-service ./apps/billing-service
COPY apps/provisioning-service ./apps/provisioning-service
COPY apps/notification-service ./apps/notification-service
COPY apps/crm-analytics-service ./apps/crm-analytics-service
COPY apps/loyalty-service ./apps/loyalty-service

RUN npm run build

CMD ["node", "apps/activation-api/dist/main.js"]
