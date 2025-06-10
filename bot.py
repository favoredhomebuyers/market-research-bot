import discord
import pandas as pd
import os
import openai
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- Setup for External APIs ---

# 1. OpenAI API Setup
# The key is loaded securely from the environment variables you set in Render.
openai.api_key = os.environ.get('OPENAI_API_KEY')

# 2. Geopy (Geocoder) Setup
# We use Nominatim, a free geocoding service based on OpenStreetMap data.
# A custom user-agent is required for their API policy.
geolocator = Nominatim(user_agent="market_research_discord_bot")
# RateLimiter prevents sending requests too quickly, which is also part of their policy.
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)


# --- Core Bot Functions ---

def get_county_from_address(address: str):
    """
    Takes a full street address and uses a geocoder to find the county.
    """
    try:
        location = geolocator.geocode(address, addressdetails=True, language="en")
        # The county information is usually found in the 'address' part of the response.
        if location and location.raw.get('address', {}).get('county'):
            county = location.raw['address']['county']
            # We also need the state abbreviation to make the county name unique.
            state = location.raw['address'].get('state', '')
            # The data is often in "CountyName County" format, so we remove the redundant part.
            county = county.replace(" County", "").strip()
            return f"{county}, {state}"
        else:
            return None
    except Exception as e:
        print(f"An error occurred during geocoding: {e}")
        return None

def analyze_market_with_ai(market_data: dict):
    """
    Sends the market data to OpenAI's GPT model for analysis and scoring.
    """
    # Create a clean string of data to send to the AI.
    data_string = ", ".join([f"{key}: {value}" for key, value in market_data.items()])
    
    # This is the "prompt" that tells the AI exactly what to do.
    prompt = f"""
    Based on the following real estate market data, please perform two tasks:
    1. Determine if it is a "Buyer's Market" or a "Seller's Market". A seller's market typically has low inventory, low days on market, and high price growth. A buyer's market is the opposite.
    2. Score the market for a real estate investor from A+ (best) to F (worst). Consider all factors like population growth, home price forecast, and cap rate. A high forecast and high cap rate are very good for an investor.

    Provide your response in the following format:
    **Analysis:** [Your determination of Buyer's or Seller's Market]
    **Investor Grade:** [Your assigned grade]
    ---
    **Reasoning:** [A brief explanation for your conclusions]

    Market Data:
    {data_string}
    """
    
    try:
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful real estate market analyst."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            max_tokens=200
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"An error occurred with the OpenAI API call: {e}")
        return "Error: Could not get an analysis from the AI."


def get_market_data_and_format(county_name: str):
    """
    This function now only retrieves the data and formats the initial embed.
    The AI analysis is handled separately.
    """
    try:
        df = pd.read_csv('merged_reventure_data.csv')
        county_data_row = df[df['County'].str.lower() == county_name.lower()]

        if not county_data_row.empty:
            data = county_data_row.iloc[0].to_dict()
            
            # This dictionary will be sent to the AI.
            data_for_ai = {
                "County": data.get("County"),
                "Population": data.get("Population"),
                "Population Growth": f"{data.get('Population_Growth')}%",
                "Days on Market": data.get('Days_on_Market'),
                "Days On Market Growth YoY": f"{data.get('Days_On_Market_Growth_YoY')}%",
                "Avg Home Value": f"${data.get('Avg_Home_Value'):,.2f}",
                "Home Sales Growth YoY": f"{data.get('Home_Sales_Growth_YoY')}%",
                "Sale Inventory Growth YoY": f"{data.get('Sale_Inventory_Growth_YoY')}%",
                "Cap Rate %": f"{data.get('Cap_Rate')}%",
                "Home Price Forecast": data.get("Home_Price_Forecast")
            }
            
            # This embed contains the raw data for the user to see.
            embed = discord.Embed(title=f"Market Data for {data.get('County')}", color=discord.Color.light_grey())
            for key, value in data_for_ai.items():
                embed.add_field(name=key, value=value if pd.notna(value) else "N/A", inline=True)
            
            return data_for_ai, embed
        else:
            return None, None
            
    except FileNotFoundError:
        embed = discord.Embed(title="Error", description="CRITICAL ERROR: `merged_reventure_data.csv` not found.", color=discord.Color.red())
        return None, embed
    except Exception as e:
        print(f"An error occurred in get_market_data: {e}")
        return None, None

# --- Main Discord Bot Logic ---

BOT_TOKEN = os.environ.get('DISCORD_TOKEN')
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f'Bot is logged in and ready as {client.user}')

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith('!market'):
        address = message.content.replace('!market', '').strip()
        
        if not address:
            await message.channel.send("Please provide a full address (e.g., `!market 155 Edinburg Dr, Kannapolis NC 28083`).")
            return

        # --- New Workflow ---
        # 1. Get County from Address
        await message.channel.send(f"Geocoding address: **{address}**...")
        county_name = get_county_from_address(address)
        
        if not county_name:
            await message.channel.send("Could not determine the county from that address. Please try a different format.")
            return

        # 2. Get Data from CSV
        await message.channel.send(f"Found County: **{county_name}**. Retrieving data...")
        data_for_ai, data_embed = get_market_data_and_format(county_name)

        if data_for_ai is None:
            # Check if an error embed was returned
            if data_embed:
                await message.channel.send(embed=data_embed)
            else:
                 await message.channel.send(f"No data found for **{county_name}** in the CSV file.")
            return
        
        # Send the raw data first
        await message.channel.send(embed=data_embed)

        # 3. Get AI Analysis
        await message.channel.send("Sending data to AI for analysis...")
        ai_analysis = analyze_market_with_ai(data_for_ai)

        # 4. Send AI Analysis
        analysis_embed = discord.Embed(title="AI Market Analysis", description=ai_analysis, color=discord.Color.blue())
        await message.channel.send(embed=analysis_embed)

# --- Run the Bot ---
if BOT_TOKEN and openai.api_key:
    client.run(BOT_TOKEN)
else:
    print("FATAL ERROR: A required token or key was not found.")
    if not BOT_TOKEN:
        print("-> DISCORD_TOKEN environment variable is missing.")
    if not openai.api_key:
        print("-> OPENAI_API_KEY environment variable is missing.")
