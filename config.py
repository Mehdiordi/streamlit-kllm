# Personal Categories Configuration
# Categorize expenses based on Target name for better tracking

personal_categories = {
    # Activities
    'Bagsvaerd Svoemmehal': 'Activities',
    'Fitnessworldas - NYX': 'Activities',
    'Planetarium': 'Activities',
    'Sportscentret': 'Activities',
    'Vestbad': 'Activities',

    # Amazon
    'Amazon': 'Amazon',
    'Amazon Web Services': 'Amazon',

    # Car Services
    'Cph Parkering a S': 'Car Services',  # Copenhagen parking
    'Clean Car': 'Car Services',
    'Ukrservice Aps': 'Car Services',
    'Øresundsbron': 'Car Services',  # Öresund Bridge toll

    # Clothes
    'Adidas': 'Clothes',
    'BabySam': 'Clothes',
    'Boozt.com': 'Clothes',
    'Decathlon': 'Clothes',
    'Mango': 'Clothes',
    'Sport 24 Outlet': 'Clothes',
    'Takko Fashion': 'Clothes',

    # Eat Out
    '7-Eleven': 'Eat Out',
    'Baba Kebab': 'Eat Out',
    'Bangkok Truck': 'Eat Out',
    'Coffee Address': 'Eat Out',
    'Copenhagen Fal': 'Eat Out',
    'Falafelo': 'Eat Out',
    'Falafillo': 'Eat Out',
    'Flere Fugle': 'Eat Out',
    'Food and Co - Novo 9P 131': 'Eat Out',
    'Grannys House': 'Eat Out',
    'Imi 2610 Aps': 'Eat Out',
    'Ismageriet': 'Eat Out',
    'Jagger - Roedovre Centrum': 'Eat Out',
    'Jammi': 'Eat Out',
    'Kiosk - Rsik - Ro - Mobile Pay': 'Eat Out',
    'Kiosk - Rsik - Rºdovr': 'Eat Out',
    'Kiosken - Rodovre - Mobile Pay': 'Eat Out',
    'Ksf Kiosk': 'Eat Out',
    'Lagkagehuset': 'Eat Out',
    "McDonald's": 'Eat Out',
    "Monop'daily": 'Eat Out',
    'Mcdnorrebrobycenter': 'Eat Out',
    'Oel Og Broed': 'Eat Out',
    'Ottoman Broenshoej Aps': 'Eat Out',
    'Restaurant Jy': 'Eat Out',

    # Extra (miscellaneous purchases)
    '4 Urbs': 'Extra',
    'Blade by J': 'Extra',
    'Gss Copenhagen': 'Extra',
    'Https Www.ladcyklen.dk': 'Extra',
    'Kerstin Nellen': 'Extra',
    'LinkedIn': 'Extra',
    'M.A.C. Cosmetics': 'Extra',
    'Magasin Du Nord': 'Extra',
    'Medium': 'Extra',
    'Royal Mermaid Amber Store': 'Extra',
    'Simply.com': 'Extra',
    'Søstrene Grene': 'Extra',
    'Sp Wuerfelhaus': 'Extra',
    'Thiele': 'Extra',

    # Fuel
    'Circle K': 'Fuel',
    'Noahs Q8 Korsør': 'Fuel',
    'Q8': 'Fuel',
    'Shell': 'Fuel',
    'Uno-X': 'Fuel',

    # Groceries
    'Aldi': 'Groceries',
    'Apotek': 'Groceries',
    'Bagt': 'Groceries',
    'Bog & idé': 'Groceries',
    'Carlsro Super': 'Groceries',
    'Coop 365': 'Groceries',
    'føtex': 'Groceries',
    'Heinemann Denmark': 'Groceries',
    'IKI': 'Groceries',  # Lithuanian grocery chain
    'Ingo': 'Groceries',
    'KaffeKapslen.dk': 'Groceries',
    'Kvickly': 'Groceries',
    'Lygten Bazar': 'Groceries',
    'Lygtenbazar Aps': 'Groceries',
    'MobilePay Danmark': 'Groceries',
    'Nan Lygten': 'Groceries',
    'Netto': 'Groceries',
    'nemlig.com': 'Groceries',
    'REMA 1000': 'Groceries',
    'Rimi': 'Groceries',  # Baltic grocery chain
    # Note: All Lidl stores are handled dynamically by checking for 'lidl' in the name

    # Home Maintenance
    'Bauhaus': 'Home Maintenance',  # Home improvement
    'Elgiganten Danmark': 'Home Maintenance',
    'IKEA': 'Home Maintenance',  # Home goods/groceries
    'Plantorama Taas': 'Home Maintenance',  # Garden center
    'Sportyfit Dk': 'Home Maintenance',
    'Thansen': 'Home Maintenance',

    # Ice Hockey
    'Holdsport': 'Ice Hockey',
    'Holdsport.dk Aps': 'Ice Hockey',
    'Mette Sauffaus': 'Ice Hockey',
    'Puk Bageri Aps': 'Ice Hockey',
    'Rexhockey': 'Ice Hockey',
    'Rodovre Mighty Bulls Aps': 'Ice Hockey',
    'Roedovre Skoejte Isho': 'Ice Hockey',
    'Rsik - Kiosk': 'Ice Hockey',
    'RØDOVRE SKØJTE & I': 'Ice Hockey',
    'Skatertown Aps': 'Ice Hockey',
    'Stine Munk': 'Ice Hockey',

    # Pet Supplies
    'Bonnie Dyrecenter Rodovre': 'Pet Supplies',
    'Borns Vilkfr - Ba - Mobile Pay': 'Pet Supplies',
    'Maxi Zoo': 'Pet Supplies',
    'Themallows.dk': 'Pet Supplies',

    # Services
    'Ladcyklen.dk - Mobile Pay': 'Services',

    # Trip (anything outside Denmark or travel-related)
    'Backstuba': 'Trip',  # Austrian bakery
    'Bahne': 'Trip',
    'Beerencafe': 'Trip',  # German cafe
    'Bolt': 'Trip',  # Ride sharing (often used abroad)
    'Booking.com': 'Trip',  # Travel booking
    'Brobizz': 'Trip',  # Bridge tolls for car
    'Deutsche Bahn': 'Trip',  # German railways
    'Dr Hermann Koehle': 'Trip',  # Austrian doctor
    'Drivers Inn': 'Trip',  # German hotel chain
    'EDEKA': 'Trip',  # German supermarket
    'Ehc Red Bull Munchen': 'Trip',  # Munich hockey
    'Eisbar Fan Eaterie Sch': 'Trip',  # German ice cream
    'Enjoy by Lillebaelt Nord': 'Trip',  # Rest stop
    'EUROSPAR': 'Trip',  # Austrian supermarket
    'Eurotrade Flughafen Mue': 'Trip',  # Munich airport
    'FC Bayern München': 'Trip',  # Munich football
    'Freizeit Arena': 'Trip',  # Austrian facility
    'Hauptfiliale Soelde': 'Trip',  # Austrian bank
    'Intersport Siebzehnrübl': 'Trip',  # German sports shop
    'Kaefer Autowelt': 'Trip',  # German car dealer
    'Katpedele': 'Trip',  # Lithuanian company
    'Kaufland': 'Trip',  # German supermarket
    'Knextgmbh - NYX': 'Trip',
    'Lalandia': 'Trip',  # Resort (could be trip)
    'Legoland': 'Trip',  # Theme park (could be trip)
    'MLCraft': 'Trip',  # German company
    'MPREIS': 'Trip',  # Austrian supermarket
    'Mountaincarts Solden': 'Trip',  # Austrian activity
    'MVG': 'Trip',  # Munich transport
    'Müller': 'Trip',  # German drugstore chain
    'Neh Svenska Ab': 'Trip',  # Swedish company
    'Netto Marken-Discount': 'Trip',  # German Netto
    'NORMAL': 'Trip',
    'Odontolog. Klinika Odonti': 'Trip',  # Lithuanian dentist
    'Oetztal Baeck': 'Trip',  # Austrian bakery
    'Panorama Restaurant': 'Trip',  # Austrian restaurant
    'Parksaeule Freizeit Arena': 'Trip',  # Austrian parking
    'PEP': 'Trip',  # German fashion
    'Pension Sportalm': 'Trip',  # Austrian accommodation
    'Raststaette Inntal West': 'Trip',  # Austrian rest stop
    'Ryanair': 'Trip',  # Airline
    'Scandlines': 'Trip',  # Ferry service
    'Schiregion Hochoetz': 'Trip',  # Austrian ski area
    'Schulranzen.com': 'Trip',  # German online shop
    'Sparda-Banken': 'Trip',  # German bank
    'Sport Riml': 'Trip',  # Austrian sports shop
    'Stadtsparkasse München - Geldautomat': 'Trip',  # Munich ATM
    'Travelis Denmark': 'Trip',  # Travel agency
    'Turkish Airlines': 'Trip',
    'Uab Ltg Link Kaunas': 'Trip',  # Lithuanian transport
    'Uab Ltg Link Vilnius': 'Trip',  # Lithuanian transport
    'Werkstatt Soelden': 'Trip',  # Austrian ski resort

    # Trip-Fuel
    'Aral': 'Trip-Fuel',
    'Esso': 'Trip-Fuel',
}

# Monthly Budget Limits Configuration
# Set different budget limits for each month (DKK)
# Keys are now strings in the format 'YYYY-MM' for robustness and easier serialization.
# Starting from October 2025, carry-over system tracks surplus/deficit between months
monthly_limits = {
    # 2025 (from October onwards - when carry-over starts)
    '2025-10': 18000,  # October 2025 (carry-over starts here)
    '2025-11': 24000,  # November 2025
    '2025-12': 26000,  # December 2025 (higher for holidays)

    # 2026 - full year
    '2026-01': 21000,   # January 2026 (higher for post-holiday spending)
    '2026-02': 21000,   # February 2026
    '2026-03': 21000,   # March 2026
    '2026-04': 21000,   # April 2026
    '2026-05': 21000,   # May 2026
    '2026-06': 21000,   # June 2026
    '2026-07': 21000,   # July 2026
    '2026-08': 21000,   # August 2026
    '2026-09': 21000,   # September 2026
    '2026-10': 21000,  # October 2026
    '2026-11': 21000,  # November 2026
    '2026-12': 21000,  # December 2026 (higher for holidays)
}

# Default monthly limit for months not specified above
default_monthly_limit = 21000


def _normalize_key_from_parts(year, month):
    """Return a 'YYYY-MM' string given year and month inputs.

    Accepts ints (year, month), a tuple (year, month), or strings for flexibility.
    """
    # If user accidentally passed a single tuple like (2025,11)
    if isinstance(year, tuple) and month is None:
        y, m = year
        year, month = y, m

    # If someone passed a single string 'YYYY-MM' as year and month is None
    if isinstance(year, str) and month in (None, ''):
        # Basic validation: ensure it's already in YYYY-MM form
        return year

    try:
        y_int = int(year)
        m_int = int(month)
    except Exception:
        # Fallback: return something that won't match any key so default is returned
        return None

    return f"{y_int:04d}-{m_int:02d}"


def get_monthly_limit(year, month):
    """Get the budget limit for a specific year and month.

    Parameters:
    - year, month: commonly passed as two ints (2025, 11). Also supports passing
      a single tuple as the first argument like (2025, 11) by leaving month=None,
      or passing a pre-formatted string year='2025-11' with month=None.

    Returns the configured limit or `default_monthly_limit` when not found.
    """
    key = _normalize_key_from_parts(year, month)
    # if not key:
    #     return default_monthly_limit

    return monthly_limits.get(key)