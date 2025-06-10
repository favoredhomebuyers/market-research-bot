import discord
import pandas as pd
import os
import openai
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- Setup for External APIs ---

openai.api_key = os.environ.get('OPENAI_API_KEY')
geolocator = Nominatim(user_agent="market_research_discord_bot_v5") # Updated user agent
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
        text = response.choices[0].message.content.strip()
        parts = text.split(',')
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
        else:
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
    except Exception as e:
        print(f"Error in get_market_data: {e}")
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

        status_message = await message.channel.send(f"Analyzing `{address}`...")
        
        county_name = get_county_from_address(address)
        if not county_name:
            await status_message.edit(content=f"❌ Could not determine the county from `{address}`.")
            return

        data = get_market_data(county_name)
        if data is None:
            await status_message.edit(content=f"❌ No data found for `{county_name}` in the CSV file.")
            return
        
        market_type, investor_grade = analyze_market_with_ai(data)
        
        # --- NEW: Build the final output as a single, plain text message ---
        
        # 1. Create the title line
        title_line = f"**{data.get('County')}: {investor_grade} ({market_type})**"

        # 2. Format all data points into clean strings
        market_stats = [
            f"Days on Market: {data.get('Days_on_Market', 'N/A')}",
            f"Days on Market Growth (YoY): {data.get('Days_On_Market_Growth_YoY', 'N/A')}%",
            f"Home Sales Growth (YoY): {data.get('Home_Sales_Growth_YoY', 'N/A')}%",
            f"Sale Inventory Growth (YoY): {data.get('Sale_Inventory_Growth_YoY', 'N/A')}%"
        ]
        demographic_stats = [
            f"Population: {data.get('Population', 'N/A'):,}",
            f"Population Growth: {data.get('Population_Growth', 'N/A')}%"
        ]
        investor_stats = [
            f"Avg Home Value: ${data.get('Avg_Home_Value', 0):,.2f}",
            f"Cap Rate %: {data.get('Cap_Rate', 'N/A')}%"
        ]
        scoring_stats = [
            f"Home Price Forecast: {data.get('Home_Price_Forecast', 'N/A')}"
        ]
        
        # 3. Assemble the final message string with bold headers
        final_message_string = (
            f"{title_line}\n\n"
            f"**Market Stats**\n" + "\n".join(market_stats) + "\n\n"
            f"**Demographic Stats**\n" + "\n".join(demographic_stats) + "\n\n"
            f"**Investor Stats**\n" + "\n".join(investor_stats) + "\n\n"
            f"**Scoring Stats**\n" + "\n".join(scoring_stats)
        )
        
        # 4. Edit the original status message to show the final result
        await status_message.edit(content=final_message_string)

# --- Run the Bot ---
if BOT_TOKEN and openai.api_key:
    client.run(BOT_TOKEN)
else:
    print("FATAL ERROR: A required token or key was not found.")
    if not BOT_TOKEN:
        print("-> DISCORD_TOKEN environment variable is missing.")
    if not openai.api_key:
        print("-> OPENAI_API_KEY environment variable is missing.")
