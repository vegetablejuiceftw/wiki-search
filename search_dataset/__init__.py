import pandas as pd
from tqdm.auto import tqdm
import unicodedata

from utils import reset_working_directory

reset_working_directory()

df: pd.DataFrame = pd.read_csv("search_dataset/search-dataset.csv")
df = df[~df["text"].isna() & ~df["id"].isna() & ~df["class"].isna()]
df = df[df["text"].str.len() > 32]
del df["text2"]
df = df.reset_index(drop=True)

data = {
    "1": "biology, reproduction, animal behavior, phallic jousting",
    "2": "agriculture, farming, soil management, plant nutrition",
    "3": "game, chess, strategy, board game",
    "4": "character, superhero, manga, anime",
    "5": "spyware, cybersecurity, digital surveillance, NSO Group",
    "6": "creature, mythology, Greek mythology, winged horse",
    "7": "company, artificial intelligence, investment, technology",
    "8": "principle, cosmology, philosophy, fine-tuning",
    "9": "machine learning, artificial intelligence, natural language processing, model benchmarking",
    "10": "material, fabric, dryer maintenance, fire safety",
    "11": "material, hygiene, belly button, body maintenance",
    "12": "software, programming, code analysis, code formatting",
    "13": "profession, musician, medieval history, entertainment",
    "14": "machine learning, artificial intelligence, natural language processing, language model",
    "15": "franchise, toys, movies, robots",
    "16": "machine learning, artificial intelligence, neural networks, natural language processing",
    "17": "electrical, power supply, voltage regulation, electronics",
    "18": "gaming, video games, cheat codes, exploit",
    "19": "relationship, infidelity, trust, communication",
    "20": "gaming, video games, unfair advantage, game manipulation",
    "21": "education, academia, academic integrity, plagiarism",
    "22": "franchise, science fiction, horror, extraterrestrial",
    "23": "lifeform, extraterrestrial, astrobiology, SETI",
    "24": "immigration, border control, visa, citizenship",
    "25": "profession, contract killing, assassination, crime",
    "26": "Hitman, video game, assassination, stealth, IO Interactive",
    "27": "Two Worlds II, RPG, role-playing game, open world, Reality Pump Studios",
    "28": "Malicious compliance, behavior, following rules, passive-aggressive, workplace dynamics",
    "29": "Beanie, clothing, knit cap, winter hat, fashion accessory",
    "30": "Beanie Boo, toy, plush toy, collectible, Ty Inc.",
    "31": "Baked, intoxication, marijuana, cannabis, getting high",
    "32": "Baking, activity, cooking, oven, recipes",
    "33": "Baking soda, material, sodium bicarbonate, household uses, cooking ingredient",
    "34": "Baked potato, dish, side dish, potato recipe, comfort food",
    "35": "Wan show, show, podcast, technology news, Linus Tech Tips",
    "36": "Credit, finance, borrowing, credit score, loans",
    "37": "Credit, attribution, recognition, acknowledgment, praise",
    "38": "Swift, singer, Taylor Swift, pop music, celebrity",
    "39": "Swift, computer science, programming language, iOS development, Apple",
    "40": "Swift, finance, SWIFT payment system, banking, international transactions",
    "41": "Python, animal, snake, reptile, species",
    "42": "Python, computer science, programming language, software development, scripting",
    "43": "Ajax, computer science, web development, JavaScript, asynchronous",
    "44": "Ajax, history, Greek mythology, hero, Trojan War",
    "45": "OOP, computer science, object-oriented programming, software design, classes",
    "46": "Infinity fabric, lithography, AMD, Ryzen, CPU architecture",
    "47": "Fabric, material, textile, cloth, sewing",
    "48": "Metadata, computer science, data, information, digital files",
    "49": "Reservation, agreement, booking, appointment, schedule",
    "50": "Tailor, profession, clothing alteration, sewing, bespoke",
    "51": "character, time traveler, TARDIS, regeneration",
    "52": "software, word processing, Microsoft Word, text editor",
    "53": "act, compassion, forgiveness, leniency, clemency",
    "54": "character, video game, healer, support, Overwatch",
    "55": "business, startup, valuation, venture capital, tech industry",
    "56": "creature, mythical, horse, fantasy, folklore",
    "57": "clothing, garment, fashion, attire, skirt design",
    "58": "character, fantasy, companion, The Witcher, bard",
    "59": "plant, weed, flower, herb, Taraxacum",
    "60": "character, outlaw, hero, folklore, Sherwood Forest",
    "61": "business, stock trading, investment, finance, brokerage",
    "62": "software, 3D modeling, animation, rendering, open-source",
    "63": "appliance, kitchen, mixer, food processor, blending",
    "64": "role, forum, poster, original poster, Reddit",
    "65": "media, entertainment, anime, opening theme, song",
    "66": "game, racquet sport, racketball, indoor sport, squash court",
    "67": "food, vegetable, gourd, pumpkin, zucchini",
    "68": "game, mind game, internet culture, meme, challenge",
    "69": "game, action role-playing, hack and slash, Blizzard Entertainment, Diablo series",
    "70": "software, text editor, development tool, coding, Microsoft",
    "71": "legal, crime, assault, felony, criminal law",
    "72": "software, framework, Rust programming language, development, coding",
    "73": "device, appliance, air circulation, cooling, remote control",
    "74": "hardware, computer component, cooling, PC fan, computer case",
    "75": "group, admirer, supporter, enthusiast, follower",
    "76": "material, lubricant, water displacement, rust prevention, penetrating oil",
    "77": "game, cue sports, billiards, snooker, pocket billiards",
    "78": "location, swimming pool, backyard, rescue, animal",
    "79": "hardware, fastener, screw, bolt, threaded",
    "80": "food, edible seed, snack, culinary, culinary nutcrackers",
    "81": "military, weaponry, defense, arms trade, conflict",
    "82": "company, technology, semiconductor, microprocessor, acquisition",
    "83": "technology, processor architecture, instruction set, CPU design",
    "84": "body part, limb, upper extremity, muscle, anatomy",
    "85": "sports, American football, gridiron, NFL, Super Bowl",
    "86": "sports, association football, soccer, Premier League, UEFA",
    "87": "software, malware, computer security, antivirus, cyber attack",
    "88": "biology, microorganism, infection, disease, epidemiology",
    "89": "communication, cryptocurrency, investment strategy, blockchain, hodling",
    "90": "math, number theory, integer, factorization, primality test",
    "91": "food, beverage, energy drink, nutrition, dietary supplement",
    "92": "condition, age, peak performance, athlete, maturity",
    "93": "sports, sumo wrestling, yokozuna, ozeki, wrestler",
    "94": "character, manga, anime, Naruto, sequel",
    "95": "country, nation, Eurasia, Middle East, Istanbul",
    "96": "animal, bird, avian, game bird, Thanksgiving",
    "97": "food, poultry, Thanksgiving, sandwich, dinner",
    "98": "chemical, heavy metal, toxicity, lead poisoning, contamination",
    "99": "award, recognition, idiocy, survival, natural selection",
    "100": "substance, psychedelic drug, hallucinogen, LSD-25, psychedelic therapy",
    "101": "computer science, data structure, binary heap, priority queue, heapify",
    "102": "military, atomic bomb, nuclear weapon, World War II, Hiroshima",
    "103": "food, beverage, energy drink, Red Bull, caffeine",
    "104": "creature, mythical creature, legendary creature, fantasy, folklore",
    "105": "race, Dungeons & Dragons, fantasy, role-playing game, tiefling traits",
    "106": "animal, bird, flightless bird, New Zealand, kiwi bird",
    "107": "food, fruit, kiwifruit, Actinidia, vitamin C",
    "108": "Astronautics, Mars, helicopter, Mars rover, space exploration",
    "109": "trait, creativity, innovation, problem-solving, resourcefulness",
    "110": "Astronautics, Mars, Mars rover, space exploration, NASA",
    "111": "trait, inquisitiveness, wonder, exploration, intellectual curiosity",
    "112": "software, network authentication, computer security, MIT Kerberos, authentication protocol",
    "113": "creature, Greek mythology, Hades, underworld, three-headed dog",
    "114": "song, music, Loreen, Eurovision Song Contest, Eurovision winner",
    "115": "art, body modification, tattoo artist, tattoo design, tattoo studio",
    "116": "instrument, woodwind instrument, flute family, orchestra, marching band",
    "117": "character, Dragon Ball, Dragon Ball Z, Namekian, anime",
    "118": "group, ethnicity, South Asia, Indian subcontinent, Indian cuisine",
    "119": "group, Native Americans, Indigenous peoples, American history, assimilation policies",
    "120": "item, nuclear physics, criticality accident, Los Alamos National Laboratory, atomic bomb development",
    "121": "software, Python, data analysis, data manipulation, data visualization",
    "122": "animal, mammal, bear, bamboo, conservation",
    "123": "military, anti-tank missile, infantry weapon, portable missile system, guided missile",
    "124": "military, main battle tank, armored vehicle, Russian military, armored warfare",
    "125": "military, main battle tank, British Army, armored vehicle, armored warfare",
    "126": "group, fellowship, organization, alliance, association",
    "127": "culture, fashion, Japanese culture, fashion trend, Zettai Ryouiki",
    "128": "product, adult toy, fantasy toy, sex toy, fantasy creature dildo",
    "129": "affliction, medical condition, epilepsy, convulsion, neurological disorder",
    "130": "legal, law enforcement, asset forfeiture, cryptocurrency seizure, Department of Justice",
    "131": "machine learning, natural language processing, information retrieval, AI model, text generation",
    "132": "item, cleaning, wiping, cloth, cleaning tool",
    "133": "gaming, video game, physics simulation, ragdoll physics, virtual characters",
}

SEARCH_DATASET = df.to_dict(orient="records")
for i, v in enumerate(SEARCH_DATASET, start=1):
    v["text"] = (
        unicodedata.normalize("NFKD", v["text"].strip())
        .encode("ascii", "ignore")
        .decode()
        .replace("  ", " ")
    )
    v["llm"] = data[str(i)]


def location(idx, arr):
    try:
        return arr.index(idx)
    except ValueError:
        return None


def evaluate(dataset, model, limit=30):
    data = []
    for row in tqdm(dataset):
        annotated_idx = set(row["id"].split(";"))
        results = model.search(row)
        result = next((r for r in results if r["wikidata_id"] in annotated_idx), default={})
        distance = result.get("distance")
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
                "distance": distance,
                # 'position': rank if rank is not None else limit,
                "rank": rank,
                "found": rank is not None,
                "ids": ids,
                **result,
            }
        )
    return data


if __name__ == "__main__":
    import json
    from itertools import islice

    data = {}
    for i, v in islice(enumerate(SEARCH_DATASET, start=1), 125, 150):
        data[i] = v.copy()
        data[i].pop("id")
        data[i]["category"] = data[i].pop("class")
        data[i]["text"] = data[i].pop("text")
        data[i].pop("llm")
    print(json.dumps(data, indent=2))
