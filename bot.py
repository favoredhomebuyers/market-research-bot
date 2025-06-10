import discord
import pandas as pd
import os
import openai
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- Setup for External APIs ---

openai.api_key = os.environ.get('OPENAI_API_KEY')
geolocator = Nominatim(user_agent="market_research_discord_bot_v8") # Updated user agent
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
    Sends market data to OpenAI for a detailed analysis, including strengths,
    weaknesses, and sales talking points.
    """
    # Now includes all data points for a more complete analysis by the AI
    data_string = (
        f"Days on Market: {market_data.get('Days_on_Market', 'N/A')}, "
        f"Home Sales Growth (YoY): {market_data.get('Home_Sales_Growth_YoY', 'N/A')}%, "
        f"Sale Inventory Growth (YoY): {market_data.get('Sale_Inventory_Growth_YoY', 'N/A')}%, "
        f"Population Growth: {market_data.get('Population_Growth', 'N/A')}%, "
        f"Avg Home Value: ${market_data.get('Avg_Home_Value', 0):,.2f}, "
        f"Home Value Growth (YoY): {market_data.get('Home_Value_Growth_YoY', 'N/A')}%, "
        f"Cap Rate: {market_data.get('Cap_Rate', 'N/A')}%, "
        f"Home Price Forecast: {market_data.get('Home_Price_Forecast', 'N/A')}, "
        f"Inventory Surplus/Deficit: {market_data.get('Inventory_Surplus_Deficit', 'N/A')}%, "
        f"Price Cut %: {market_data.get('Price_Cut_Percentage', 'N/A')}%, "
        f"Vacancy Rate: {market_data.get('Vacancy_Rate', 'N/A')}%, "
        f"Housing Unit Growth Rate: {market_data.get('Housing_Unit_Growth_Rate', 'N/A')}%"
    )
    
    prompt = f"""
    You are an expert real estate investment analyst and sales coach for a virtual real estate wholesaling company. 

    **Market Data:**
    {data_string}

    ---

    **PART 1: Market Analysis**
    Provide a concise analysis of this market's health based on the data.
    - **Strengths:** Identify 1-2 positive indicators.
    - **Weaknesses:** Identify 1-2 negative indicators that present challenges for a typical seller.
    - **Overall Health:** A brief summary conclusion.

    **PART 2: Wholesaling Offer Strategy**
    Provide a strategic recommendation for making offers.
    - **Approach:** State if the strategy should be "Cautious/Lower Offers" or "Aggressive/Higher Offers".
    - **Justification:** Briefly explain why, referencing the market data.
    - **Exit Strategy Note:** Determine whether market conditions are more suited selling to cash investors or retail buyers or neither or both and why.  

    Structure your response using the following headers.

    **Market Analysis**
    * **Strengths:**
    * **Weaknesses:**
    * **Overall Health:**

    **Seller Conversation Starters**
    * **Situationn Question:**
    * **Problem Question:**
    * **Implication Question:**
    * **Need Payoff Question:**
    """
    
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert real estate analyst and sales coach."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.6,
            max_tokens=500
        )
        content = response.choices[0].message.content
        return content if content else "AI analysis returned empty. There might be an issue with the prompt or a content filter."
    except Exception as e:
        print(f"An error occurred with the OpenAI API call: {e}")
        return f"Error: Could not get analysis from AI. Details: {str(e)}"

def get_market_data(county_name: str):
    """
    Retrieves and returns the raw data dictionary for a county.
    """
    try:
        df = pd.read_csv('merged_reventure_data.csv')
        county_data_row = df[df['County'].str.lower() == county_name.lower()]
        return county_data_row.iloc[0].to_dict() if not county_data_row.empty else None
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

        status_message = await message.channel.send(f"🔬 Analyzing `{address}`...")
        
        county_name = get_county_from_address(address)
        if not county_name:
            await status_message.edit(content=f"❌ Could not determine the county from `{address}`.")
            return

        data = get_market_data(county_name)
        if data is None:
            await status_message.edit(content=f"❌ No data found for `{county_name}` in the CSV file.")
            return
        
        # --- NEW: Display all raw data points in the first message ---
        
        raw_data_string = (
            f"**Market Stats**\n"
            f"Days on Market: {data.get('Days_on_Market', 'N/A')}\n"
            f"Days on Market Growth (YoY): {data.get('Days_On_Market_Growth_YoY', 'N/A')}%\n"
            f"Home Sales Growth (YoY): {data.get('Home_Sales_Growth_YoY', 'N/A')}%\n"
            f"Sale Inventory Growth (YoY): {data.get('Sale_Inventory_Growth_YoY', 'N/A')}%\n"
            f"Inventory Surplus/Deficit: {data.get('Inventory_Surplus_Deficit', 'N/A')}%\n"
            f"Price Cut %: {data.get('Price_Cut_Percentage', 'N/A')}%\n\n"
            f"**Demographic Stats**\n"
            f"Population: {data.get('Population', 'N/A'):,}\n"
            f"Population Growth: {data.get('Population_Growth', 'N/A')}%\n"
            f"Median Age: {data.get('Median_Age', 'N/A')}\n\n"
            f"**Investor Stats**\n"
            f"Avg Home Value: ${data.get('Avg_Home_Value', 0):,.2f}\n"
            f"Home Value Growth (YoY): {data.get('Home_Value_Growth_YoY', 'N/A')}%\n"
            f"Cap Rate %: {data.get('Cap_Rate', 'N/A')}%\n"
            f"Vacancy Rate: {data.get('Vacancy_Rate', 'N/A')}%\n"
            f"Housing Unit Growth Rate: {data.get('Housing_Unit_Growth_Rate', 'N/A')}%\n\n"
            f"**Scoring Stats**\n"
            f"Home Price Forecast: {data.get('Home_Price_Forecast', 'N/A')}"
        )
        
        data_embed = discord.Embed(
            title=f"Raw Data for {data.get('County')}",
            description=raw_data_string,
            color=discord.Color.light_grey()
        )
        
        await status_message.edit(content="✅ Data found! Here are the stats:")
        await message.channel.send(embed=data_embed)

        thinking_message = await message.channel.send("🤖 Now sending to AI for analysis and talking points...")
        ai_analysis = analyze_market_with_ai(data)
        
        analysis_embed = discord.Embed(
            title="AI Analysis & Conversation Starters",
            description=ai_analysis,
            color=discord.Color.blue()
        )
        await thinking_message.edit(content="", embed=analysis_embed)


# --- Run the Bot ---
if BOT_TOKEN and openai.api_key:
    client.run(BOT_TOKEN)
else:
    print("FATAL ERROR: A required token or key was not found.")
    if not BOT_TOKEN:
        print("-> DISCORD_TOKEN environment variable is missing.")
    if not openai.api_key:
        print("-> OPENAI_API_KEY environment variable is missing.")
