# --- Stage 1: use Dart SDK to build a standalone executable ---
FROM dart:stable AS build

WORKDIR /app

# First, we copy pubspec files and get dependencies (this allows caching if they haven't changed)
COPY pubspec.* ./
RUN dart pub get

# Then, we copy the rest of the application code (touch .env to avoid errors if it doesn't exist)
COPY . .
RUN touch .env

# Compile the server to get a standalone executable
RUN dart compile exe bin/server.dart -o bin/server

# Write the current server version (from pubspec.yaml) and latest commit hash to a file for later use
RUN grep '^version:' pubspec.yaml | awk '{print $2}' > CURRENT_VERSION.txt  || echo unknown > CURRENT_VERSION.txt
RUN git config --global --add safe.directory /app
RUN git rev-parse HEAD > LATEST_COMMIT_HASH.txt 2>/dev/null || echo unknown > LATEST_COMMIT_HASH.txt

# --- Stage 2: create a minimal image with the precompiled executable ---
FROM alpine:3.20

# Add gcompat and libstdc++ for compatibility with the Dart executable
RUN apk add --no-cache gcompat libstdc++

WORKDIR /app

# Only compile the precedently built executable from the build stage, and the .env if it exists
COPY --from=build --chown=10001:10001 /app/bin/server /app/bin/server
COPY --from=build /app/CURRENT_VERSION.txt /app/CURRENT_VERSION.txt
COPY --from=build /app/LATEST_COMMIT_HASH.txt /app/LATEST_COMMIT_HASH.txt
COPY --from=build /app/.env /app/.env

# Create a non-root user to run the application
RUN adduser -D -H appuser
USER appuser

# Set a default port and expose it (may be overridden by .env)
ENV PORT=8080
EXPOSE 8080

CMD ["/app/bin/server"]