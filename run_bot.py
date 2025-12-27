import os
import asyncio
from discord.ext import commands


intents = commands.Intents.default()
intents.message_content = True
intents.voice_states = True

bot = commands.Bot(command_prefix='/', intents=intents)


@bot.event
async def on_ready():
    print("Bot ready:", bot.user)


async def main():
    # import and setup the song cog (uses async setup function)
    try:
        from song import setup as setup_song
        await setup_song(bot)
    except Exception:
        # fallback: try adding cog directly
        try:
            from song import MusicCog
            await bot.add_cog(MusicCog(bot))
        except Exception as e:
            print("Warning: could not load song cog:", e)

    TOKEN = os.environ.get("DISCORD_TOKEN")
    if not TOKEN:
        raise RuntimeError("Set DISCORD_TOKEN environment variable.")
    await bot.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
import os
import discord
from discord.ext import commands
from song import MusicCog

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix='/', intents=intents)

@bot.event
async def on_ready():
    print("Bot ready:", bot.user)

    async def setup():
        await bot.add_cog(MusicCog(bot))

        # modern discord.py: add cogs inside setup_hook
        @bot.event
        async def setup_hook():
            await bot.add_cog(MusicCog(bot))

            TOKEN = os.environ.get("DISCORD_TOKEN")
            if not TOKEN:
                raise RuntimeError("Set DISCORD_TOKEN environment variable.")
                bot.run(TOKEN)