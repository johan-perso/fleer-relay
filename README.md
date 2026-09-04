<!-- ###### Version française [ici](https://github.com/johan-perso/fleer-relay/blob/main/README.fr.md). -->

# Fleer | Relay Server

Everything you dream about a one-to-one file sharing service: easy, privacy-first, decentralized and cross-platform, without limits.

> This GitHub repository contains the source code required to run a relay server. Related projects can be found on the [@johan-perso](https://github.com/johan-perso?tab=repositories&q=fleer-) profile.

> [!IMPORTANT]  
> This project is still in development and is far from being production-ready. Tbh you should not even try to compile it yet.  
> I'm just sharing my progress for those who are curious.


## Hosting

### Run with Docker Compose (easiest)

**Prerequisites:**

- [Docker Compose](https://docs.docker.com/compose/install/)

1. Create a `docker-compose.yml` file with the following content:

```yaml
version: "3.9"
services:
  fleer-relay:
    image: ghcr.io/johan-perso/fleer-relay:latest
    restart: unless-stopped
    ports:
      - "80:8080"
    env_file:
      - .env
```

2. Download and edit the [`.env.example`](https://github.com/johan-perso/fleer-relay/blob/main/.env.example) file to set your own values, then rename it to `.env`.

3. Run the following command to start the service in the background:

```bash
docker compose up -d
```

### Other hosting methods

<details>
<summary>Build and run a Docker image</summary>

**Prerequisites:**

- [Docker](https://docs.docker.com/get-docker/)
- [Git CLI](https://git-scm.com/)

```bash
# Clone the repository and navigate into it
git clone https://github.com/johan-perso/fleer-relay.git
cd fleer-relay

# Edit the configuration to set your own values
cp .env.example .env
open .env

# Build a Docker image (one time), then run it
docker build -t experimental-fleer-relay .
docker run -p 80:8080 -d --restart unless-stopped --name experimental-fleer-relay experimental-fleer-relay
```
</details>

<details>
<summary>Run from source</summary>

**Prerequisites:**
- [Dart SDK](https://dart.dev/get-dart)
- [Git CLI](https://git-scm.com/)

```bash
# Clone the repository and navigate into it
git clone https://github.com/johan-perso/fleer-relay.git
cd fleer-relay

# Edit the configuration to set your own values
cp .env.example .env
open .env

# Install the dependencies and run the program
dart pub get
dart run bin/server.dart # will run in the foreground, you can stop it with Ctrl+C
```

> To run it in the background, you can use `nohup`, `screen`, or `tmux`.  
> For usage on servers, it is recommended to use a built executable (`dart compile exe bin/server.dart`) instead of `dart run`, or to use Docker.  
> In case of fatal errors, the program is made to automatically stop itself, you need to use a process manager like `systemd` or `pm2` to restart it automatically.

</details>

## License

MIT © [Johan](https://johanstick.fr/). [Support this project](https://johanstick.fr/#donate) if you want to help me 💙