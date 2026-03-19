import random
import uuid
import json


# ----------------------------------
# INDIAN VISUAL DNA SYSTEM
# ----------------------------------

BASE_STYLE = (
    "cinematic, ultra realistic, 8k, "
    "Indian color palette saffron teal marigold orange, "
    "blend of traditional Indian motifs with futuristic technology, "
    "diverse Indian people in smart clothing, "
    "natural lighting, dramatic composition"
)


# ----------------------------------
# SCENE POOLS - FULLY INDIANIZED
# ----------------------------------

HOOK_SCENES = [
    "futuristic Indian megacity at sunrise lotus-shaped skyscrapers flying vehicles with rangoli LED patterns diverse Indian crowd",
    "advanced AI research center India engineers in kurtas with holographic interfaces temple architecture fused with glass buildings morning light",
    "Indian spaceport with rocket launching ISRO logo visible traditional lamp ceremony happening crowd cheering dramatic lighting",
    "smart village India 2060 traditional huts with solar roofs robots helping farmers green fields sunrise warm colors",
    "futuristic Mumbai skyline with marine drive preserved anti-gravity vehicles saffron and teal color scheme morning mist",
    "Bengaluru tech campus 2060 traditional courtyard design with holographic trees engineers in sarees and suits"
]

TREND_SCENES = [
    "AI hospital India doctor in white coat with AR glasses examining patient Ganesh statue in background clean modern interior soft lighting",
    "Indian classroom 2060 students in uniform with tablets AI teacher hologram Sanskrit script on digital blackboard bright lighting",
    "high-tech Indian railway station bullet train with peacock feather design passengers in diverse Indian clothing digital signage Hindi English",
    "vertical farm Mumbai traditional marigold flowers growing in hydroponic towers Indian farmer monitoring with tablet sunset colors",
    "smart ghat Varanasi floating platforms on Ganges holographic aarti ceremony pilgrims with tech wearables evening lights",
    "futuristic Jaipur market holographic vendors traditional Rajasthani architecture anti-gravity delivery drones busy crowd"
]

INSIGHT_SCENES = [
    "quantum computer Indian lab circuit patterns resembling mandala scientist in saree with safety goggles blue gold lighting",
    "Indian ocean cleanup operation robotic boats with traditional boat designs Mumbai skyline background morning mist",
    "desert solar farm Rajasthan panels arranged in geometric patterns like rangoli camel in foreground golden hour",
    "Indian manufacturing robot arms decorated with mehndi patterns factory floor with diya lamps warm industrial lighting",
    "AI research lab Hyderabad traditional courtyard with holographic data displays scientists in kurta-pajamas with AR glasses",
    "underwater research station near Kochi marine biologists in smart suits traditional boat preserved in museum glass"
]

FUTURE_SCENES = [
    "Mars colony with Indian flag dome habitat with temple architecture astronaut in suit with Om symbol patch red planet landscape",
    "underwater city off Kerala coast glass domes with Kerala boat design marine life Indian family looking out blue-green lighting",
    "floating city above Ganges river platforms with Varanasi ghats design holy men with tech wearables sunrise golden light",
    "Himalayan research station snow-capped peaks monastery fused with observatory prayer flags with solar panels clear sky",
    "Lunar base ISRO Chandrayaan memorial Indian astronauts doing yoga in low gravity Earth visible through dome",
    "asteroid mining operation Indian spacecraft with traditional patterns zero gravity factory workers in smart suits"
]

ENDING_SCENES = [
    "panoramic view India 2060 diverse landscapes mountains to ocean Taj Mahal preserved with holographic protection sunset patriotic colors",
    "generation of Indians elder with traditional clothes youth in smart wear child with AR glasses all looking at futuristic city golden hour emotional",
    "Indian flag waving on moon base earthrise in background astronaut doing namaste vast space awe-inspiring",
    "time lapse futuristic Indian city day to night Diwali lights coming on family celebrating on balcony warm colors",
    "aerial view smart India network connections between villages and cities glowing like neural network dawn lighting",
    "hopeful Indian child looking at holographic globe touching India on map futuristic bedroom warm lighting emotional"
]


# ----------------------------------
# REGIONAL VARIATIONS (Optional Enhancement)
# ----------------------------------

REGIONAL_ELEMENTS = {
    "north": "Himalayan backdrop Mughal architecture influences Punjabi vibrant colors",
    "south": "temple gopurams coconut trees Dravidian patterns Tamil Nadu coastal",
    "west": "desert solar farms Gujarati stepwell architecture Maharashtra forts",
    "east": "Sundarbans mangroves Bengali terracotta motifs Kolkata heritage",
    "central": "tribal art influences Madhya Pradesh forests ancient caves"
}


# ----------------------------------
# PROMPT BUILDER
# ----------------------------------

def build_prompt(scene, topic, region=None):
    """Build final prompt with Indian DNA"""
    
    prompt = f"{scene}, {topic}, {BASE_STYLE}"
    
    # Add regional flavor if specified
    if region and region in REGIONAL_ELEMENTS:
        prompt += f", {REGIONAL_ELEMENTS[region]}"
    
    return prompt


# ----------------------------------
# SCENE OBJECT BUILDER
# ----------------------------------

def build_scene(scene_type, scene_pool, topic, region=None):
    """Build single scene object"""
    
    scene = random.choice(scene_pool)
    
    return {
        "scene_type": scene_type,
        "prompt": build_prompt(scene, topic, region),
        "duration": 5,
        "indian_elements": extract_indian_elements(scene)
    }


def extract_indian_elements(scene):
    """Tag scene with Indian elements for tracking"""
    
    elements = []
    keywords = {
        "temple": "religious_architecture",
        "saree": "traditional_clothing",
        "kurta": "traditional_clothing",
        "ISRO": "space_pride",
        "Ganesh": "religious_symbol",
        "rangoli": "folk_art",
        "mehndi": "folk_art",
        "Diwali": "festival",
        "Om": "spiritual",
        "namaste": "gesture",
        "chai": "daily_life",
        "auto-rickshaw": "transport",
        "ghat": "riverside_culture",
        "banyan": "nature",
        "monsoon": "climate",
        "Himalayan": "geography",
        "desert": "geography",
        "coastal": "geography"
    }
    
    scene_lower = scene.lower()
    for keyword, category in keywords.items():
        if keyword.lower() in scene_lower:
            elements.append(f"{category}:{keyword}")
    
    return elements


# ----------------------------------
# PROMPT GENERATOR
# ----------------------------------

def generate_scenes(topic, region=None):
    """
    Generate 5 cinematic scenes for video
    
    Args:
        topic: The video topic
        region: Optional region for localization ('north', 'south', 'west', 'east', 'central')
    """
    
    scenes = [
        build_scene("hook", HOOK_SCENES, topic, region),
        build_scene("trend", TREND_SCENES, topic, region),
        build_scene("insight", INSIGHT_SCENES, topic, region),
        build_scene("future", FUTURE_SCENES, topic, region),
        build_scene("ending", ENDING_SCENES, topic, region)
    ]
    
    return scenes


# ----------------------------------
# JOB PROCESSOR
# ----------------------------------

def process_job(job):
    """
    Process job and generate visual scenes
    
    Input job: {
        "job_id": "uuid",
        "topic": "AI doctors in India",
        "region": "south"  // optional
    }
    
    Output job: {
        "job_id": "uuid",
        "topic": "AI doctors in India",
        "scenes": [...],
        "visual_prompts": [...],
        "status": "visual_prompts_ready",
        "indian_element_score": 8.5
    }
    """
    
    topic = job.get("topic", "Future India")
    region = job.get("region")  # Optional regional targeting
    
    # Generate scenes
    scenes = generate_scenes(topic, region)
    
    # Calculate Indianization score
    all_elements = []
    for scene in scenes:
        all_elements.extend(scene.get("indian_elements", []))
    
    unique_elements = len(set(all_elements))
    indian_score = min(10, unique_elements * 1.5)  # Max 10
    
    # Update job
    job["scenes"] = scenes
    job["visual_prompts"] = [scene["prompt"] for scene in scenes]
    job["status"] = "visual_prompts_ready"
    job["indian_element_score"] = round(indian_score, 1)
    job["indian_elements_used"] = list(set(all_elements))
    
    return job


# ----------------------------------
# TEST RUN
# ----------------------------------

if __name__ == "__main__":
    
    # Test 1: Generic topic
    job1 = {
        "job_id": str(uuid.uuid4()),
        "topic": "AI doctors in India"
    }
    
    job1 = process_job(job1)
    
    print("\n" + "="*70)
    print("TEST 1: Generic Topic")
    print("="*70)
    print(f"\nTopic: {job1['topic']}")
    print(f"Indian Score: {job1['indian_element_score']}/10")
    print(f"Elements: {', '.join(job1['indian_elements_used'])}")
    print("\nGenerated Scenes:")
    for scene in job1["scenes"]:
        print(f"\n  [{scene['scene_type'].upper()}]")
        print(f"  Prompt: {scene['prompt'][:80]}...")
        print(f"  Tags: {scene['indian_elements']}")
    
    # Test 2: Regional topic (South India)
    job2 = {
        "job_id": str(uuid.uuid4()),
        "topic": "Smart cities in South India",
        "region": "south"
    }
    
    job2 = process_job(job2)
    
    print("\n" + "="*70)
    print("TEST 2: South India Regional")
    print("="*70)
    print(f"\nTopic: {job2['topic']}")
    print(f"Region: {job2.get('region', 'none')}")
    print(f"Indian Score: {job2['indian_element_score']}/10")
    print("\nHook Scene Prompt:")
    print(job2["scenes"][0]["prompt"])
