import requests
from bs4 import BeautifulSoup
import geojson
import re
from typing import Dict, List
import time
import os

BASE_URL = "https://www.waterwayguide.com"

def parse_coordinates(coord_str: str) -> tuple:
    """Parse coordinates from format 'N 30° 48.185' / W 088° 00.884'' to decimal degrees"""
    lat_match = re.search(r'([NS])\s*(\d+)°\s*(\d+\.\d+)', coord_str)
    lon_match = re.search(r'([EW])\s*(\d+)°\s*(\d+\.\d+)', coord_str)
    
    if not (lat_match and lon_match):
        return None, None
    
    lat_dir, lat_deg, lat_min = lat_match.groups()
    lon_dir, lon_deg, lon_min = lon_match.groups()
    
    lat = float(lat_deg) + float(lat_min) / 60
    lon = float(lon_deg) + float(lon_min) / 60
    
    if lat_dir == 'S':
        lat = -lat
    if lon_dir == 'W':
        lon = -lon
        
    return lat, lon

def get_bridge_details(bridge_url: str) -> Dict:
    """Scrape details for a specific bridge"""
    try:
        response = requests.get(bridge_url)
        if response.status_code != 200:
            print(f"Error accessing bridge URL {bridge_url}: {response.status_code}")
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        details = soup.find('div', id='navaid-details')
        
        if not details:
            print(f"No details found for bridge {bridge_url}")
            return None
            
        bridge_info = {}
        bridge_info['name'] = details.find('h1').text.strip()
        print(f"Processing bridge: {bridge_info['name']}")
        
        # Extract all list items
        info_items = details.find_all('li')
        for item in info_items:
            text = item.text.strip()
            if 'Mile Marker' in text:
                bridge_info['mile_marker'] = text.split(':')[1].strip()
            elif 'Lat / Lon' in text:
                bridge_info['coordinates_raw'] = text.split(':')[1].strip()
                lat, lon = parse_coordinates(text.split(':')[1].strip())
                bridge_info['latitude'] = lat
                bridge_info['longitude'] = lon
            elif 'Bridge Type' in text:
                bridge_info['bridge_type'] = text.split(':')[1].strip()
            elif 'Vertical Clearance' in text:
                bridge_info['vertical_clearance'] = text.split(':')[1].strip()
            elif 'Horizontal Clearance' in text:
                bridge_info['horizontal_clearance'] = text.split(':')[1].strip()
            elif 'VHF Channel' in text:
                bridge_info['vhf_channel'] = text.split(':')[1].strip()
            elif 'Schedule' in text:
                bridge_info['schedule'] = item.find('p').text.strip() if item.find('p') else ''
                
        return bridge_info
    except Exception as e:
        print(f"Error getting bridge details for {bridge_url}: {str(e)}")
        return None

def get_bridges_for_town(town_url: str) -> List[Dict]:
    """Get all bridges for a specific town"""
    try:
        response = requests.get(town_url)
        if response.status_code != 200:
            print(f"Error accessing town URL {town_url}: {response.status_code}")
            return []
            
        soup = BeautifulSoup(response.text, 'html.parser')
        bridges = []
        
        bridge_items = soup.find_all('div', class_='directory-item', attrs={'navaidtypeslug': 'bridge'})
        print(f"Found {len(bridge_items)} bridges in {town_url.split('/')[-1]}")
        
        for bridge in bridge_items:
            bridge_type_id = bridge.get('navaidtypeid')
            bridge_id = bridge.get('navaidid')
            bridge_slug = bridge.get('navaidslug')
            
            if all([bridge_type_id, bridge_id, bridge_slug]):
                bridge_url = f"{BASE_URL}/bridge/{bridge_type_id}-{bridge_id}/{bridge_slug}"
                bridge_info = get_bridge_details(bridge_url)
                if bridge_info:
                    bridges.append(bridge_info)
                    time.sleep(1)  # Be nice to the server
                
        return bridges
    except Exception as e:
        print(f"Error getting bridges for {town_url}: {str(e)}")
        return []

def get_towns_for_state(state_url: str) -> List[str]:
    """Get all town URLs for a state"""
    try:
        response = requests.get(state_url)
        if response.status_code != 200:
            print(f"Error accessing state URL {state_url}: {response.status_code}")
            return []
            
        soup = BeautifulSoup(response.text, 'html.parser')
        town_links = soup.find_all('a', class_='list-group-item', href=re.compile(r'/directory/bridge-lock/.*/.+'))
        town_urls = [BASE_URL + link['href'] for link in town_links]
        print(f"Found {len(town_urls)} towns in {state_url.split('/')[-1]}")
        return town_urls
    except Exception as e:
        print(f"Error getting towns for {state_url}: {str(e)}")
        return []

def get_all_states() -> List[str]:
    """Get all state URLs"""
    states = [
        'alabama', 'alaska', 'california', 'connecticut', 'delaware', 
        'florida', 'georgia', 'hawaii', 'louisiana', 'maine', 'maryland',
        'massachusetts', 'michigan', 'minnesota', 'mississippi', 
        'new-hampshire', 'new-jersey', 'new-york', 'north-carolina',
        'ohio', 'oregon', 'pennsylvania', 'rhode-island', 'south-carolina',
        'texas', 'virginia', 'washington', 'wisconsin'
    ]
    state_urls = [f"{BASE_URL}/directory/bridge-lock/{state}" for state in states]
    print(f"Processing {len(state_urls)} states: {states}")
    return state_urls

def create_geojson(bridges: List[Dict], filename: str) -> None:
    """Create GeoJSON from bridge data"""
    features = []
    
    for bridge in bridges:
        if bridge.get('latitude') and bridge.get('longitude'):
            feature = geojson.Feature(
                geometry=geojson.Point((bridge['longitude'], bridge['latitude'])),
                properties={
                    'name': bridge.get('name'),
                    'mile_marker': bridge.get('mile_marker'),
                    'bridge_type': bridge.get('bridge_type'),
                    'vertical_clearance': bridge.get('vertical_clearance'),
                    'horizontal_clearance': bridge.get('horizontal_clearance'),
                    'vhf_channel': bridge.get('vhf_channel'),
                    'schedule': bridge.get('schedule')
                }
            )
            features.append(feature)
    
    feature_collection = geojson.FeatureCollection(features)
    
    # Create output directory if it doesn't exist
    os.makedirs('bridge_data', exist_ok=True)
    output_path = os.path.join('bridge_data', filename)
    
    with open(output_path, 'w') as f:
        geojson.dump(feature_collection, f, indent=2)

def main():
    # Get all state URLs
    state_urls = get_all_states()
    
    for state_url in state_urls:
        state_name = state_url.split('/')[-1]
        print(f"Processing state: {state_name}")
        
        state_bridges = []
        town_urls = get_towns_for_state(state_url)
        
        for town_url in town_urls:
            print(f"Processing town: {town_url}")
            bridges = get_bridges_for_town(town_url)
            state_bridges.extend(bridges)
            time.sleep(1)  # Be nice to the server
        
        # Save state data to its own file
        if state_bridges:
            create_geojson(state_bridges, f"{state_name}_bridges.geojson")
            print(f"Saved {len(state_bridges)} bridges for {state_name}")
    
    print("Completed processing all states!")

if __name__ == "__main__":
    main()
