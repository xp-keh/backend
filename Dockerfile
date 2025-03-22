# Use an official Node.js base image
FROM node:20-alpine

RUN apk add --no-cache curl

# Set working directory
WORKDIR /usr/src/app

# Install dependencies
COPY package*.json ./
RUN npm install

# Copy rest of the code
COPY . .

# Start the app
CMD ["npm", "start"]
