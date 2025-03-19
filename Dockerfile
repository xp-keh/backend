# Use official Node.js LTS as a parent image
FROM node:18

# Set working directory
WORKDIR /usr/src/app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install app dependencies
RUN npm install

# Copy the rest of your application code
COPY . .

# Expose the correct port
EXPOSE 4000

# Ensure node_modules is accessible when using volumes
RUN chmod -R 777 /usr/src/app/node_modules

# Command to run your app
CMD ["npm", "start"]
