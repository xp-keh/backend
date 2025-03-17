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

# Expose the port your app runs on (e.g., 3000)
EXPOSE 3000

# Command to run your app
CMD [ "npm", "start" ]
