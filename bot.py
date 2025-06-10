import discord
import pandas as pd
import os
import openai
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import re

# --- Setup for External APIs ---

openai.api_key = os.environ.get('OPENAI_API_KEY')
geolocator = Nominatim(user_agent="market_research_discord_bot_v4") # Updated user agent
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)


# --- Core Bot Functions ---

def get_county_from_address(address: str):
    """
    Takes a full street address, finds the county, and ensures the state
    is a two-letter abbreviation to match the CSV file format.
    """
    states = {
        'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR', 
        'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE', 
        'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID', 
        'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS', 
        'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD', 
        'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS', 
        'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV', 
        'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY', 
        'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK', 
        'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC', 
        'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT', 
        'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV', 
        'wisconsin': 'WI', 'wyoming': 'WY'
    }
    
    try:
        location = geolocator.geocode(address, addressdetails=True, language="en")
        if location and location.raw.get('address', {}).get('county'):
            county = location.raw['address']['county']
            state_full_name = location.raw['address'].get('state', '').lower()
            state_abbr = states.get(state_full_name)
            
            if not state_abbr:
                return None

            county = county.replace(" County", "").strip()
            return f"{county}, {state_abbr}"
        else:
            return None
    except Exception as e:
        print(f"An error occurred during geocoding: {e}")
        return None

def analyze_market_with_ai(market_data: dict):
    """
    Sends data to OpenAI to get an investor grade and market type.
    """
    data_string = ", ".join([f"{key}: {value}" for key, value in market_data.items()])
    
    prompt = f"""
    Based on the following real estate market data, provide only two things:
    1. The market type ("Buyer's Market" or "Seller's Market").
    2. An investor grade from A+ to F.

    Return the result on a single line, formatted exactly like this:
    Market Type, Investor Grade

    Example: Seller's Market, B+

    Market Data:
    {data_string}
    """
    
    try:
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a real estate analyst that provides concise, formatted output."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=50
        )
        # Parse the response to get Market Type and Grade
        text = response.choices[0].message.content.strip()
        parts = text.split(',')
        if len(parts) == 2:
            market_type = parts[0].strip()
            investor_grade = parts[1].strip()
            return market_type, investor_grade
        else:
            # Fallback if the model doesn't respond as expected
            return "N/A", "N/A"
    except Exception as e:
        print(f"An error occurred with the OpenAI API call: {e}")
        return "Error", "Error"

def get_market_data(county_name: str):
    """
    Retrieves and returns the raw data dictionary for a county.
    """
    try:
        df = pd.read_csv('merged_reventure_data.csv')
        county_data_row = df[df['County'].str.lower() == county_name.lower()]

        if not county_data_row.empty:
            return county_data_row.iloc[0].to_dict()
        else:
            return None
            
    except FileNotFoundError:
        print("CRITICAL ERROR: `merged_reventure_data.csv` not found.")
        return None
    except Exception as e:
        print(f"An error occurred in get_market_data: {e}")
        return None

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
            await message.channel.send("Please provide a full address.")
            return

        status_message = await message.channel.send(f"**Working on it...**\n> 🌍 Geocoding address: `{address}`")

        county_name = get_county_from_address(address)
        
        if not county_name:
            await status_message.edit(content=f"❌ Could not determine the county from `{address}`.")
            return

        await status_message.edit(content=f"**Working on it...**\n> ✅ Found County: `{county_name}`\n> 📊 Retrieving data...")
        data = get_market_data(county_name)

        if data is None:
            await status_message.edit(content=f"❌ No data found for `{county_name}` in the CSV file.")
            return
        
        await status_message.edit(content=f"**Working on it...**\n> ✅ Data retrieved!\n> 🤖 Sending to AI for analysis...")
        market_type, investor_grade = analyze_market_with_ai(data)
        
        # --- NEW: Build the final embed with custom formatting ---
        
        # Format all the data strings first
        population_str = f"Population: {data.get('Population'):,}" if pd.notna(data.get('Population')) else "Population: N/A"
        pop_growth_str = f"Population Growth: {data.get('Population_Growth')}%" if pd.notna(data.get('Population_Growth')) else "Population Growth: N/A"
        
        days_on_market_str = f"Days on Market: {data.get('Days_on_Market')}" if pd.notna(data.get('Days_on_Market')) else "Days on Market: N/A"
        dom_yoy_str = f"Days on Market Growth (YoY): {data.get('Days_On_Market_Growth_YoY')}%" if pd.notna(data.get('Days_On_Market_Growth_YoY')) else "Days on Market Growth (YoY): N/A"
        sales_yoy_str = f"Home Sales Growth (YoY): {data.get('Home_Sales_Growth_YoY')}%" if pd.notna(data.get('Home_Sales_Growth_YoY')) else "Home Sales Growth (YoY): N/A"
        inventory_yoy_str = f"Sale Inventory Growth (YoY): {data.get('Sale_Inventory_Growth_YoY')}%" if pd.notna(data.get('Sale_Inventory_Growth_YoY')) else "Sale Inventory Growth (YoY): N/A"
        
        home_value_str = f"Avg Home Value: ${data.get('Avg_Home_Value'):,.2f}" if pd.notna(data.get('Avg_Home_Value')) else "Avg Home Value: N/A"
        cap_rate_str = f"Cap Rate %: {data.get('Cap_Rate')}%" if pd.notna(data.get('Cap_Rate')) else "Cap Rate %: N/A"

        forecast_str = f"Home Price Forecast: {data.get('Home_Price_Forecast')}" if pd.notna(data.get('Home_Price_Forecast')) else "Home Price Forecast: N/A"
        market_type_str = f"Market Type: {market_type}"
        
        # Assemble the description string with bold headers
        description = (
            f"**Market Stats**\n"
            f"{days_on_market_str}\n{dom_yoy_str}\n{sales_yoy_str}\n{inventory_yoy_str}\n\n"
            f"**Demographic Stats**\n"
            f"{population_str}\n{pop_growth_str}\n\n"
            f"**Investor Stats**\n"
            f"{home_value_str}\n{cap_rate_str}\n\n"
            f"**Scoring Stats**\n"
            f"{forecast_str}\n{market_type_str}"
        )
        
        # Create and send the final embed
        final_embed = discord.Embed(
            title=f"{data.get('County')}: {investor_grade}",
            description=description,
            color=discord.Color.green()
        )
        await message.channel.send(embed=final_embed)
        
        # Delete the original "Working on it..." message
        await status_message.delete()

# --- Run the Bot ---
if BOT_TOKEN and openai.api_key:
    client.run(BOT_TOKEN)
else:
    print("FATAL ERROR: A required token or key was not found.")
    if not BOT_TOKEN:
        print("-> DISCORD_TOKEN environment variable is missing.")
    if not openai.api_key:
        print("-> OPENAI_API_KEY environment variable is missing.")
