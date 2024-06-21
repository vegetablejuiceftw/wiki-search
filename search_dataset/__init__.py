import pandas as pd
from tqdm.auto import tqdm
import unicodedata

from search_dataset.llm_expansion import complete_prompt
from utils import reset_working_directory

reset_working_directory()

df: pd.DataFrame = pd.read_csv("search_dataset/search-dataset.csv")
df = df[~df["text"].isna() & ~df["id"].isna() & ~df["class"].isna()]
df = df[df["text"].str.len() > 32]
del df["text2"]
df = df.reset_index(drop=True)

data = {
    # "1": "biology, reproduction, animal behavior, phallic jousting",
    # "2": "agriculture, farming, soil management, plant nutrition",
    # "3": "game, chess, strategy, board game",
    # "4": "character, superhero, manga, anime",
    # "5": "spyware, cybersecurity, digital surveillance, NSO Group",
    # "6": "creature, mythology, Greek mythology, winged horse",
    # "7": "company, artificial intelligence, investment, technology",
    # "8": "principle, cosmology, philosophy, fine-tuning",
    # "9": "machine learning, artificial intelligence, natural language processing, model benchmarking",
    # "10": "material, fabric, dryer maintenance, fire safety",
    # "11": "material, hygiene, belly button, body maintenance",
    # "12": "software, programming, code analysis, code formatting",
    # "13": "profession, musician, medieval history, entertainment",
    # "14": "machine learning, artificial intelligence, natural language processing, language model",
    # "15": "franchise, toys, movies, robots",
    # "16": "machine learning, artificial intelligence, neural networks, natural language processing",
    # "17": "electrical, power supply, voltage regulation, electronics",
    # "18": "gaming, video games, cheat codes, exploit",
    # "19": "relationship, infidelity, trust, communication",
    # "20": "gaming, video games, unfair advantage, game manipulation",
    # "21": "education, academia, academic integrity, plagiarism",
    # "22": "franchise, science fiction, horror, extraterrestrial",
    # "23": "lifeform, extraterrestrial, astrobiology, SETI",
    # "24": "immigration, border control, visa, citizenship",
    # "25": "profession, contract killing, assassination, crime",
    # "26": "Hitman, video game, assassination, stealth, IO Interactive",
    # "27": "Two Worlds II, RPG, role-playing game, open world, Reality Pump Studios",
    # "28": "Malicious compliance, behavior, following rules, passive-aggressive, workplace dynamics",
    # "29": "Beanie, clothing, knit cap, winter hat, fashion accessory",
    # "30": "Beanie Boo, toy, plush toy, collectible, Ty Inc.",
    # "31": "Baked, intoxication, marijuana, cannabis, getting high",
    # "32": "Baking, activity, cooking, oven, recipes",
    # "33": "Baking soda, material, sodium bicarbonate, household uses, cooking ingredient",
    # "34": "Baked potato, dish, side dish, potato recipe, comfort food",
    # "35": "Wan show, show, podcast, technology news, Linus Tech Tips",
    # "36": "Credit, finance, borrowing, credit score, loans",
    # "37": "Credit, attribution, recognition, acknowledgment, praise",
    # "38": "Swift, singer, Taylor Swift, pop music, celebrity",
    # "39": "Swift, computer science, programming language, iOS development, Apple",
    # "40": "Swift, finance, SWIFT payment system, banking, international transactions",
    # "41": "Python, animal, snake, reptile, species",
    # "42": "Python, computer science, programming language, software development, scripting",
    # "43": "Ajax, computer science, web development, JavaScript, asynchronous",
    # "44": "Ajax, history, Greek mythology, hero, Trojan War",
    # "45": "OOP, computer science, object-oriented programming, software design, classes",
    # "46": "Infinity fabric, lithography, AMD, Ryzen, CPU architecture",
    # "47": "Fabric, material, textile, cloth, sewing",
    # "48": "Metadata, computer science, data, information, digital files",
    # "49": "Reservation, agreement, booking, appointment, schedule",
    # "50": "Tailor, profession, clothing alteration, sewing, bespoke",
    # "51": "character, time traveler, TARDIS, regeneration",
    # "52": "software, word processing, Microsoft Word, text editor",
    # "53": "act, compassion, forgiveness, leniency, clemency",
    # "54": "character, video game, healer, support, Overwatch",
    # "55": "business, startup, valuation, venture capital, tech industry",
    # "56": "creature, mythical, horse, fantasy, folklore",
    # "57": "clothing, garment, fashion, attire, skirt design",
    # "58": "character, fantasy, companion, The Witcher, bard",
    # "59": "plant, weed, flower, herb, Taraxacum",
    # "60": "character, outlaw, hero, folklore, Sherwood Forest",
    # "61": "business, stock trading, investment, finance, brokerage",
    # "62": "software, 3D modeling, animation, rendering, open-source",
    # "63": "appliance, kitchen, mixer, food processor, blending",
    # "64": "role, forum, poster, original poster, Reddit",
    # "65": "media, entertainment, anime, opening theme, song",
    # "66": "game, racquet sport, racketball, indoor sport, squash court",
    # "67": "food, vegetable, gourd, pumpkin, zucchini",
    # "68": "game, mind game, internet culture, meme, challenge",
    # "69": "game, action role-playing, hack and slash, Blizzard Entertainment, Diablo series",
    # "70": "software, text editor, development tool, coding, Microsoft",
    # "71": "legal, crime, assault, felony, criminal law",
    # "72": "software, framework, Rust programming language, development, coding",
    # "73": "device, appliance, air circulation, cooling, remote control",
    # "74": "hardware, computer component, cooling, PC fan, computer case",
    # "75": "group, admirer, supporter, enthusiast, follower",
    # "76": "material, lubricant, water displacement, rust prevention, penetrating oil",
    # "77": "game, cue sports, billiards, snooker, pocket billiards",
    # "78": "location, swimming pool, backyard, rescue, animal",
    # "79": "hardware, fastener, screw, bolt, threaded",
    # "80": "food, edible seed, snack, culinary, culinary nutcrackers",
    # "81": "military, weaponry, defense, arms trade, conflict",
    # "82": "company, technology, semiconductor, microprocessor, acquisition",
    # "83": "technology, processor architecture, instruction set, CPU design",
    # "84": "body part, limb, upper extremity, muscle, anatomy",
    # "85": "sports, American football, gridiron, NFL, Super Bowl",
    # "86": "sports, association football, soccer, Premier League, UEFA",
    # "87": "software, malware, computer security, antivirus, cyber attack",
    # "88": "biology, microorganism, infection, disease, epidemiology",
    # "89": "communication, cryptocurrency, investment strategy, blockchain, hodling",
    # "90": "math, number theory, integer, factorization, primality test",
    # "91": "food, beverage, energy drink, nutrition, dietary supplement",
    # "92": "condition, age, peak performance, athlete, maturity",
    # "93": "sports, sumo wrestling, yokozuna, ozeki, wrestler",
    # "94": "character, manga, anime, Naruto, sequel",
    # "95": "country, nation, Eurasia, Middle East, Istanbul",
    # "96": "animal, bird, avian, game bird, Thanksgiving",
    # "97": "food, poultry, Thanksgiving, sandwich, dinner",
    # "98": "chemical, heavy metal, toxicity, lead poisoning, contamination",
    # "99": "award, recognition, idiocy, survival, natural selection",
    # "100": "substance, psychedelic drug, hallucinogen, LSD-25, psychedelic therapy",
    # "101": "computer science, data structure, binary heap, priority queue, heapify",
    # "102": "military, atomic bomb, nuclear weapon, World War II, Hiroshima",
    # "103": "food, beverage, energy drink, Red Bull, caffeine",
    # "104": "creature, mythical creature, legendary creature, fantasy, folklore",
    # "105": "race, Dungeons & Dragons, fantasy, role-playing game, tiefling traits",
    # "106": "animal, bird, flightless bird, New Zealand, kiwi bird",
    # "107": "food, fruit, kiwifruit, Actinidia, vitamin C",
    # "108": "Astronautics, Mars, helicopter, Mars rover, space exploration",
    # "109": "trait, creativity, innovation, problem-solving, resourcefulness",
    # "110": "Astronautics, Mars, Mars rover, space exploration, NASA",
    # "111": "trait, inquisitiveness, wonder, exploration, intellectual curiosity",
    # "112": "software, network authentication, computer security, MIT Kerberos, authentication protocol",
    # "113": "creature, Greek mythology, Hades, underworld, three-headed dog",
    # "114": "song, music, Loreen, Eurovision Song Contest, Eurovision winner",
    # "115": "art, body modification, tattoo artist, tattoo design, tattoo studio",
    # "116": "instrument, woodwind instrument, flute family, orchestra, marching band",
    # "117": "character, Dragon Ball, Dragon Ball Z, Namekian, anime",
    # "118": "group, ethnicity, South Asia, Indian subcontinent, Indian cuisine",
    # "119": "group, Native Americans, Indigenous peoples, American history, assimilation policies",
    # "120": "item, nuclear physics, criticality accident, Los Alamos National Laboratory, atomic bomb development",
    # "121": "software, Python, data analysis, data manipulation, data visualization",
    # "122": "animal, mammal, bear, bamboo, conservation",
    # "123": "military, anti-tank missile, infantry weapon, portable missile system, guided missile",
    # "124": "military, main battle tank, armored vehicle, Russian military, armored warfare",
    # "125": "military, main battle tank, British Army, armored vehicle, armored warfare",
    # "126": "group, fellowship, organization, alliance, association",
    # "127": "culture, fashion, Japanese culture, fashion trend, Zettai Ryouiki",
    # "128": "product, adult toy, fantasy toy, sex toy, fantasy creature dildo",
    # "129": "affliction, medical condition, epilepsy, convulsion, neurological disorder",
    # "130": "legal, law enforcement, asset forfeiture, cryptocurrency seizure, Department of Justice",
    # "131": "machine learning, natural language processing, information retrieval, AI model, text generation",
    # "132": "item, cleaning, wiping, cloth, cleaning tool",
    # "133": "gaming, video game, physics simulation, ragdoll physics, virtual characters",
}

# data = {
#     "0": "Biology: Fertilizing eggs after phallic jousting match, motherhood consolation.",
#     "1": "Agriculture: Less is more in fertilizing, emphasizing micronutrient balance.",
#     "2": "Game: Strategic maneuvering in a game involving a king piece.",
#     "3": "Character: King, the strongest hero on earth, sought by others for aid.",
#     "4": "Spyware: Pegasus, an Israeli intelligence asset, suffers a critical failure.",
#     "5": "Creature: Pegasus, a mythical winged horse, its real mythology often overlooked.",
#     "6": "Company: Google invests in Anthropic, rival to OpenAI.",
#     "7": "Principle: Anthropic Principle, related to the concept of super-intelligence.",
#     "8": "Machine learning: Anthropically focused model, Claude 2.0, shows improvement.",
#     "9": "Material: Lint accumulation poses fire hazard, notably in dryer ducts.",
#     "10": "Material: Belly button lint, a mixture of fibers and hair, can cause infections.",
#     "11": "Software: Linter dependencies include ESLint and Prettier for code quality.",
#     "12": "Profession: Bard, historically known for entertainment and storytelling.",
#     "13": "Machine learning: Bard model, distinct from Bing, shows emerging differences.",
#     "14": "Franchise: Transformers, encompassing various characters and stories.",
#     "15": "Machine learning: Transformer neural network contextualizes character relationships.",
#     "16": "Electrical: Compatibility with 8-24v AC Transformers indicated for device usage.",
#     "17": "Gaming: Cheating enhances gaming experience, celebrated as top-tier strategy.",
#     "18": "Relationship: Cheating condemned as morally wrong, with gender differences in motivations.",
#     "19": "Gaming: Cheating in games grants players superior knowledge, often at high skill levels.",
#     "20": "Education: Academic dishonesty, exemplified by cheating, poses challenges for professors.",
#     "21": "Franchise: Alien series movies depict encounters with extraterrestrial beings.",
#     "22": "Lifeform: Hypothetical encounters with aliens raise questions on appearance and communication.",
#     "23": "Immigration: DHS lacks criminal history data on aliens from countries that don't report.",
#     "24": "Profession: Hitman hired for nefarious purposes, exemplified by criminal cases.",
#     "25": "Game: Hitman 2 offers diverse strategies and options for completing objectives.",
#     "26": "Game: Two Worlds Two, an RPG, becomes playable after anticipation.",
#     "27": "Behaviour: Malicious compliance rewarded after strict adherence to rules.",
#     "28": "Clothing: Beanie adorned with cat ears, described as super cute.",
#     "29": "Toy: Beanie Boo collection involves grouping by color, such as red.",
#     "30": "Intoxication: Getting high in Washington State, referencing cannabis consumption.",
#     "31": "Activity: Baking cookies at 350°F, preparing multiple ovens for the task.",
#     "32": "Material: Baking soda uses include heartburn relief and natural deodorant.",
#     "33": "Dish: Classic baked potato, simple preparation with rubbed oil.",
#     "34": "Show: Wan Show transcripts provide content for chat prompts.",
#     "35": "Finance: Tanked credit despite efforts to build, now at 700.",
#     "36": "Attribution: Republicans claim credit for actions after voting out.",
#     "37": "Singer: Taylor Swift, prominent figure in popular culture.",
#     "38": "Computer science: Learn Swift programming, suitable for beginners.",
#     "39": "Finance: Kiev angered over EU's Swift payment system indecision.",
#     "40": "Animal: 30-foot Python found in Indonesia, charred carcass discovered.",
#     "41": "Computer science: Django web framework using Python for web apps.",
#     "42": "Computer science: Ajax crash course, vital in web development.",
#     "43": "History: Ajax in Atlantis, witness to its destruction, aided by Athena.",
#     "44": "Computer science: OOP emphasizes code as objects, essential paradigm.",
#     "45": "Lithography: Infinity Fabric links CPU to RAM, tied to RAM frequency.",
#     "46": "Material: Dollar Tree offers varied fabric rolls in different colors.",
#     "47": "Computer science: Metadata inconsistencies in images, editing software effects.",
#     "48": "Agreement: Secure dining reservation during off-peak times for convenience.",
#     "49": "Profession: Tailor alters clothing to exact measurements for perfect fit.",
#     "50": "Character: Doctor's relationship with TARDIS, pivotal in storyline.",
#     "51": "Software: WordPad saves document as doc file for later use.",
#     "52": "Act: Subject to audience's mercy, particularly on YouTube.",
#     "53": "Character: Overwatch characters include Mercy, Genji, and Hanzo.",
#     "54": "Business: Pitching AI unicorn startup, addressing blank costs.",
#     "55": "Creature: Befriending a unicorn, riding through magical lands together.",
#     "56": "Clothing: Reviewing three sexy skirts from Amazon, pleased with quality.",
#     "57": "Character: Geralt and Dandelion fishing, encounter massive catfish.",
#     "58": "Plant: Wild dandelion greens offer nourishment and survival food source.",
#     "59": "Character: Robin Hood depicted as a vicious figure in early tales.",
#     "60": "Business: Robinhood app democratizes investment, founded by Stanford graduates.",
#     "61": "Software: Blender render time measured, baseline entry at 15.8 minutes.",
#     "62": "Appliance: Blender used for making banana smoothies, emits noise.",
#     "63": "Role: OP's friend attempts to steal boyfriend, r/relationshipadvice scenario.",
#     "64": "Media: Different versions of English dubbed anime OP themes.",
#     "65": "Game: Fast-paced squash requires racquet, ball, court, and opponent.",
#     "66": "Food: Squash fruits harvested in 2-3 weeks, simple process.",
#     "67": "Game: Internet challenge: mentioning 'the game' leads to loss.",
#     "68": "Game: Diablo 4 game doesn't require monthly subscription, one-time purchase.",
#     "69": "Software: VS Code extension 'Prettier' for code formatting.",
#     "70": "Legal: Aggravated mayhem charge for torturing husband amid divorce.",
#     "71": "Software: Bevy framework, Rust programming language.",
#     "72": "Device: Remote-controlled fan regulates speed, oscillation, airflow.",
#     "73": "Hardware: Phanteks 120mm T30 fan claims superiority over Noctua NFA 12.",
#     "74": "Group: Being a fan of an artist's music or work.",
#     "75": "Material: WD-40, not a lubricant, but a water displacer.",
#     "76": "Game: Crystal, traditional pool player on World Snooker tour.",
#     "77": "Location: Backyard pool used for baby deer rescue.",
#     "78": "Hardware: Nut threaded onto lead screw, requiring removal.",
#     "79": "Food: Shelling nuts for consumption, historic nutcrackers at Leavenworth.",
#     "80": "Military: Decline in Russian arms exports under Putin's leadership.",
#     "81": "Company: Apple's influence on ARM spun-off company in 1990.",
#     "82": "Technology: ARM processors in smartphones, incompatible with x86.",
#     "83": "Body Part: Completing exercises for complete arm pump.",
#     "84": "Sports: NFL assistant coaches build dossiers during college football careers.",
#     "85": "Sports: Contenders for UK's most hated football club.",
#     "86": "Software: Downloading computer viruses, background changes after recovery.",
#     "87": "Biology: Skin's barrier against cold viruses, prevents infection.",
#     "88": "Communication: Glassnode's hodl wave indicator for analyzing crypto market.",
#     "89": "Math: Determining prime numbers, challenging identification.",
#     "90": "Food: Prime energy drink, granting powerful new abilities.",
#     "91": "Condition: Prime years for defenders, understanding game and body.",
#     "92": "Sports: European-born sumo wrestlers achieving titles.",
#     "93": "Character: Boruto apologizes for actions, injured arms bandaged.",
#     "94": "Country: Acquisition of F-16s by Turkey under Erdogan's leadership.",
#     "95": "Animal: Turkey elusive in hunting scenario.",
#     "96": "Food: Turkey used in sturdy sandwich ingredients.",
#     "97": "Chemical: Upset over water containing lead, concerns over poisoning.",
#     "98": "Award: Darwin award earned by reckless actions, lack of safety precautions.",
#     "99": "Substance: LSD used for brainwashing and manipulation in Cold War.",
#     "100": "Computer Science: Heap deletion method, removing only root element.",
#     "101": "Military: Fat Man bomb, nuclear explosion caused by plutonium.",
#     "102": "Food: Monster energy drink, popularizing energy drinks since late '90s.",
#     "103": "Creature: Unique monsters in RPG, undefeated by any person.",
#     "104": "Race: Tiefling halfling variant, chaotic nature with demonic background.",
#     "105": "Animal: Kiwi egg size compared to body, not largest egg.",
#     "106": "Food: Nutritional benefits of kiwi fruit, high vitamin C content.",
#     "107": "Astronautics: Mars rover Ingenuity's communication loss during emergency landing.",
#     "108": "Trait: Hero's ingenuity, ability to think outside the box.",
#     "109": "Astronautics: NASA's Curiosity rover, rocket-powered sky crane landing.",
#     "110": "Trait: Embracing curiosity, superior to following passion.",
#     "111": "Software: Kerberos network authentication protocol.",
#     "112": "Creature: Cerberus, guard dog of Hades, prevents escape from underworld.",
#     "113": "Song: Factors shaping 'Tattoo' by Loreen.",
#     "114": "Art: Tattoo artist's humanity and part of tattooing process.",
#     "115": "Instrument: Piccolo's role in orchestra, enhancing sound.",
#     "116": "Character: Piccolo referencing Frieza in a conversation.",
#     "117": "Group: Percentage of Indians practicing vegetarianism.",
#     "118": "Group: American assimilation strategy for dealing with Indians.",
#     "119": "Item: Demon core criticality accident, nuclear chain reaction.",
#     "120": "Software: Python course dependencies including pandas library.",
#     "121": "Animal: Panda's herbivorous diet despite order name Carnivora.",
#     "122": "Military: Javelin missile system, portable anti-tank weapon.",
#     "123": "Military: T-90 tank's relevance in modern warfare.",
#     "124": "Military: British contribution of Challenger 2 tanks to Ukraine.",
#     "125": "Group: Nafo's role in countering Russian disinformation.",
#     "126": "Culture: Absolute territory, strip of thigh visible below skirt.",
#     "127": "Product: Bad Dragon dildo received as a holiday bonus.",
#     "128": "Affliction: Witness experiencing a seizure during police interaction.",
#     "129": "Legal: Financial seizure related to virtual currency exchange hack.",
#     "130": "Machine Learning: RAG model, retrieving and generating from passages.",
#     "131": "Item: Wiping dust off a panel using a rag soaked in mineral spirits.",
#     "132": "Gaming: Simulation of battles using rag dolls for entertainment.",
# }


def location(idx, arr):
    try:
        return arr.index(idx)
    except ValueError:
        return None


def evaluate(dataset, model, limit=30):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('display.max_colwidth', None)

    data = []
    for row in tqdm(dataset):
        annotated_idx = set(row["id"].split(";"))
        results = model.search(row)
        result = next((r for r in results if r["wikidata_id"] in annotated_idx), {})
        # distance = result.get("distance")
        ids = [r["wikidata_id"] for r in results]
        rank = min((location(qid, ids) for qid in annotated_idx if qid in ids), default=None)

        data.append(
            {
                "source": model.source,
                "method": model.method,
                "search_limit": getattr(model, "search_limit", None),
                **row,
                "target": annotated_idx,
                "candidates": len(ids),
                # "distance": distance,
                # 'position': rank if rank is not None else limit,
                "rank": rank,
                "found": rank is not None,
                "ids": ids,
                **result,
            }
        )
    return data


SEARCH_DATASET = df.to_dict(orient="records")
SEARCH_DATASET = SEARCH_DATASET#[:64]

prompt_template = """\
    You will be writing a short, universal definition of a term found in provided context. The goal is
    to create a definition that could be used to add the term to Wikidata for future use.

    You will be provided with the following inputs:
    "{name}" - The name of the term you need to define.
    "{class}" - The category or domain the term belongs to.
    "{text}" - A short paragraph of context about the term.

    First, review the provided inputs carefully. Think about how you can use the name, category, and
    context information to write a clear, concise definition of the term.

    Next, write a short, universal definition of the term, approximately 7 words long. The
    definition should be general enough to be useful for adding the term to Wikidata, but specific
    enough to accurately capture the meaning of the term. Do not start the definition with the term name.

    Only output your definition inside <definition> tags."""

gaslight = "As depicted in the provided context the definition is: <definition>"

for i, v in enumerate(SEARCH_DATASET, start=0):
    v["text"] = (
        unicodedata.normalize("NFKD", v["text"].strip())
        .encode("ascii", "ignore")
        .decode()
        .replace("  ", " ")
    )
    response = complete_prompt(v, prompt_template, gaslight)
    response = response.replace("</", "<").replace("<definition>", "")
    v['llm'] = response
    if v['id'] in [
        # 'Q22909116',
        # 'Q1151299',
        # 'Q63437015', 'Q55418044', 'Q5339301', 'Q23118', 'Q48485', 'Q528974',
        # 'Q63437015', 'Q55418044', 'Q5339301', 'Q528974', 'Q33602', 'Q22909116', 'Q536118',
        'Q63437015', 'Q5339301', 'Q22909116', 'Q33602', 'Q536118',
        # 'Q63437015', 'Q215144', 'Q1348417;Q19', 'Q55418044', 'Q5339301', 'Q728', 'Q848706', 'Q23118', 'Q48485',
        # 'Q63437015', 'Q215144', 'Q1348417;Q19', 'Q19308', 'Q19308', 'Q131219', 'Q55418044', 'Q5339301', 'Q11020', 'Q22909116', 'Q848706', 'Q19357667', 'Q48485',
        # 'Q63437015', 'Q215144', 'Q19308', 'Q107789646', 'Q242468', 'Q193432', 'Q46513625', 'Q22909116', 'Q43', 'Q848706', 'Q708', 'Q19357667', 'Q48485', 'Q366791',
    ]:
        print(str([i, v['id'], v['name'], response])[1:-1])
        # print(v)
print()