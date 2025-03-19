# Use official Node.js LTS as a parent image
FROM node:18

# Set working directory
WORKDIR /usr/src/app

# Copy only package.json and package-lock.json first (better caching)
COPY package*.json ./

# Install app dependencies
RUN npm install

# Copy the rest of the application source code
COPY . .

# Ensure node_modules is accessible when using volumes
RUN chmod -R 777 /usr/src/app/node_modules

# Expose the correct port
EXPOSE 4000

# Command to run your app
CMD ["npm", "start"]
