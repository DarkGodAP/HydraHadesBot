# HydraHadesBot

Quick start
-----------

1. Create and activate a virtualenv:

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

Note: `youtubesearchpython` is optional and not included in `requirements.txt` because it may not be available on all systems. If you want the optional search helper, install it manually:

```bash
pip install youtubesearchpython
```

3. Provide your secrets (edit `vars.sh` or set env vars):

```bash
source vars.sh
# or
export DISCORD_TOKEN="your_token"
export SPOTIFY_CLIENT_ID="..."
export SPOTIFY_CLIENT_SECRET="..."
```

4. Run the bot:

```bash
python run_bot.py
```

Security: never commit real tokens to the repo.

