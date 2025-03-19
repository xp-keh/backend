# Use official Node.js LTS as a parent image
FROM node:18

# Set working directory
WORKDIR /usr/src/app

# Copy package.json and package-lock.json first for better caching
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the application source code
COPY . .

# Expose the correct port
EXPOSE 4000

# Fix permission issues (optional)
RUN chmod -R 777 /usr/src/app/node_modules

# Command to run your app
CMD ["npm", "start"]
