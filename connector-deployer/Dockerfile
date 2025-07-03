# Use a lightweight base image
FROM alpine:latest

# Install bash, curl, and jq
RUN apk add --no-cache bash curl jq

# Copy the Bash script into the container
COPY deploy-connector.sh /deploy-connector.sh

# Make the script executable
RUN chmod +x /deploy-connector.sh

# Set the script as the entrypoint
ENTRYPOINT ["/deploy-connector.sh"]
