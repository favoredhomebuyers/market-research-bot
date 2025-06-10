import discord
import pandas as pd
import os

# --- Core Function: Get Market Data ---
def get_market_data(county_name: str):
    """
    Reads the merged CSV file and retrieves data for a specific county.
    Formats the data into a Discord Embed object for a nice-looking reply.
    """
    try:
        # The bot will read the single, clean CSV file from the repository.
        df = pd.read_csv('merged_reventure_data.csv')
        # This makes the search case-insensitive for a better user experience.
        county_data = df[df['County'].str.lower() == county_name.lower()]

        if not county_data.empty:
            data = county_data.iloc[0].to_dict()
            
            # Create a user-friendly "Embed" to display the data neatly in Discord.
            embed = discord.Embed(
                title=f"Market Research for {data.get('County')}",
                description="Data sourced from Reventure App.",
                color=discord.Color.blue()
            )
            embed.add_field(name="County Population", value=f"{data.get('Population'):,}" if pd.notna(data.get('Population')) else "N/A", inline=True)
            embed.add_field(name="Avg Home Value", value=f"${data.get('Avg_Home_Value'):,.2f}" if pd.notna(data.get('Avg_Home_Value')) else "N/A", inline=True)
            embed.add_field(name="Vacancy Rate %", value=f"{data.get('Vacancy_Rate')}%" if pd.notna(data.get('Vacancy_Rate')) else "N/A", inline=True)
            
            embed.add_field(name="Days on Market", value=data.get('Days_on_Market') if pd.notna(data.get('Days_on_Market')) else "N/A", inline=True)
            embed.add_field(name="Days On Market Growth (YoY)", value=f"{data.get('Days_On_Market_Growth_YoY')}%" if pd.notna(data.get('Days_On_Market_Growth_YoY')) else "N/A", inline=True)
            embed.add_field(name="Price Cut %", value=f"{data.get('Price_Cut_Percentage')}%" if pd.notna(data.get('Price_Cut_Percentage')) else "N/A", inline=True)

            embed.add_field(name="Sale Inventory Growth (YoY)", value=f"{data.get('Sale_Inventory_Growth_YoY')}%" if pd.notna(data.get('Sale_Inventory_Growth_YoY')) else "N/A", inline=True)
            embed.add_field(name="Sale Inventory Growth (MoM)", value=f"{data.get('Sale_Inventory_Growth_MoM')}%" if pd.notna(data.get('Sale_Inventory_Growth_MoM')) else "N/A", inline=True)
            embed.add_field(name="Home Sales Growth (YoY)", value=f"{data.get('Home_Sales_Growth_YoY')}%" if pd.notna(data.get('Home_Sales_Growth_YoY')) else "N/A", inline=True)
            
            embed.add_field(name="Population Growth", value=f"{data.get('Population_Growth')}%" if pd.notna(data.get('Population_Growth')) else "N/A", inline=True)
            embed.add_field(name="Cap Rate %", value=f"{data.get('Cap_Rate')}%" if pd.notna(data.get('Cap_Rate')) else "N/A", inline=True)
            embed.add_field(name="Home Price Forecast", value=data.get("Home_Price_Forecast") if pd.notna(data.get("Home_Price_Forecast")) else "N/A", inline=True)
            
            embed.set_footer(text="Market Research Bot")
            return embed
        else:
            # If the county isn't found, log it for future review.
            with open('missing_counties.log', 'a') as f:
                f.write(f"{county_name}\n")
            return "No Data Found"
            
    except FileNotFoundError:
        return "CRITICAL ERROR: `merged_reventure_data.csv` not found in the repository."
    except Exception as e:
        print(f"An error occurred in get_market_data: {e}")
        return "An internal error occurred. Please check the logs."

# --- Discord Bot Setup and Events ---
# It's best practice to get the token from the hosting environment, not to write it in the code.
BOT_TOKEN = os.environ.get('DISCORD_TOKEN')

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

@client.event
async def on_ready():
    """This event runs when the bot successfully connects to Discord."""
    print(f'Bot is logged in and ready as {client.user}')

@client.event
async def on_message(message):
    """This event runs every time a message is sent in the server."""
    # Ignore messages sent by the bot itself.
    if message.author == client.user:
        return

    # Check if the message starts with the command '!market'.
    if message.content.startswith('!market'):
        # Get the county name by removing the command part and any extra spaces.
        county_name = message.content.replace('!market', '').strip()
        
        if not county_name:
            await message.channel.send("Please provide a county and state (e.g., `!market Orange, CA`).")
            return

        # Let the user know the bot is working on their request.
        await message.channel.send(f"Searching for market data for **{county_name}**...")
        
        # Call our function to get the data.
        result = get_market_data(county_name)

        # Send the result back to the Discord channel.
        if isinstance(result, discord.Embed):
            await message.channel.send(embed=result)
        else:
            await message.channel.send(result)

# --- Final Check and Bot Run ---
if BOT_TOKEN:
    client.run(BOT_TOKEN)
else:
    print("FATAL ERROR: The DISCORD_TOKEN was not found.")
    print("You must set this as an Environment Variable in your Render dashboard.")
