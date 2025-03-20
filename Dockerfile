# Use official Node.js LTS as a base image
FROM node:18

# Set working directory
WORKDIR /usr/src/app

# Copy package.json and package-lock.json for better build caching
COPY package*.json ./

# Install dependencies efficiently
RUN npm install --only=production

# Copy the rest of the application source code
COPY . .

# Set environment variable (optional, recommended for production)
ENV NODE_ENV=production

# Expose the correct application port
EXPOSE 4000

# Ensure correct file permissions (if required)
RUN chown -R node:node /usr/src/app

# Switch to non-root user for security
USER node

# Command to start the application
CMD ["npm", "start"]
