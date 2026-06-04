FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY . .
# db-listener does not expose an HTTP port; placeholder for future health endpoint
EXPOSE 3001
CMD ["npm", "start"]
