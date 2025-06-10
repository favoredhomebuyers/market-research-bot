import discord
import pandas as pd
import os
import openai
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- Setup for External APIs ---

openai.api_key = os.environ.get('OPENAI_API_KEY')
geolocator = Nominatim(user_agent="market_research_discord_bot_v6") # Updated user agent
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
    weaknesses, and sales talking points based on Sandler, SPIN, and NLP.
    """
    # Create a clean string of the most relevant data for the AI prompt.
    data_string = (
        f"Days on Market: {market_data.get('Days_on_Market', 'N/A')}, "
        f"Home Sales Growth (YoY): {market_data.get('Home_Sales_Growth_YoY', 'N/A')}%, "
        f"Sale Inventory Growth (YoY): {market_data.get('Sale_Inventory_Growth_YoY', 'N/A')}%, "
        f"Population Growth: {market_data.get('Population_Growth', 'N/A')}%, "
        f"Avg Home Value: ${market_data.get('Avg_Home_Value', 0):,.2f}, "
        f"Cap Rate: {market_data.get('Cap_Rate', 'N/A')}%, "
        f"Home Price Forecast: {market_data.get('Home_Price_Forecast', 'N/A')}"
    )
    
    # This is the new, much more detailed prompt.
    prompt = f"""
    You are an expert real estate sales coach. Your task is to analyze market data and provide talking points for a real estate wholesaler on a call with a potential seller.

    **Market Data:**
    {data_string}

    ---

    **PART 1: Market Analysis**
    Provide a concise analysis of this market's health.
    - **Strengths:** Identify 1-2 positive indicators from the data.
    - **Weaknesses:** Identify 1-2 negative indicators that create selling challenges.
    - **Overall Health:** A brief summary conclusion.

    **PART 2: Wholesaler Persuasion Script**
    Based *only* on the market weaknesses you identified, create talking points and questions to influence a seller to accept a discounted, fast cash offer. Incorporate the following methods:
    1.  **SPIN Selling:** Create questions to explore the seller's Situation, Problem, the Implication of that problem, and the Need-Payoff of a fast sale.
    2.  **Sandler Method:** Suggest ways to uncover the seller's true "pain" or motivation beyond just the money.
    3.  **NLP (Neuro-Linguistic Programming):** Use positive framing and influential language.

    Structure your response exactly as follows:

    **Market Analysis**
    * **Strengths:** [Your analysis]
    * **Weaknesses:** [Your analysis]
    * **Overall Health:** [Your analysis]

    **Wholesaler Talking Points**
    * **Uncover Pain (Sandler):** [Your suggested question]
    * **Explore Problem (SPIN):** [Your suggested question based on a weakness]
    * **Amplify Implication (SPIN):** [Your suggested question about the consequence of the problem]
    * **Frame the Solution (NLP):** [Your suggested statement]
    """
    
    try:
        response = openai.chat.completions.create(
            model="gpt-4o", # Using a more advanced model for better results
            messages=[
                {"role": "system", "content": "You are an expert real estate analyst and sales coach."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=450 # Increased tokens for a more detailed response
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"An error occurred with the OpenAI API call: {e}")
        return "Error: Could not get an analysis from the AI."

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

        status_message = await message.channel.send(f"🔬 Analyzing `{address}`...")
        
        county_name = get_county_from_address(address)
        if not county_name:
            await status_message.edit(content=f"❌ Could not determine the county from `{address}`.")
            return

        data = get_market_data(county_name)
        if data is None:
            await status_message.edit(content=f"❌ No data found for `{county_name}` in the CSV file.")
            return
        
        # Now, the analysis is the main payload.
        ai_analysis = analyze_market_with_ai(data)
        
        # Assemble the final message
        final_message = (
            f"### Analysis for {data.get('County')}\n\n"
            f"{ai_analysis}"
        )
        
        # Edit the status message to show the final result
        # Note: Discord has a 2000 character limit for messages. This detailed analysis should fit.
        await status_message.edit(content=final_message)

# --- Run the Bot ---
if BOT_TOKEN and openai.api_key:
    client.run(BOT_TOKEN)
else:
    print("FATAL ERROR: A required token or key was not found.")
    if not BOT_TOKEN:
        print("-> DISCORD_TOKEN environment variable is missing.")
    if not openai.api_key:
        print("-> OPENAI_API_KEY environment variable is missing.")
