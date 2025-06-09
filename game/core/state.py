import logging
import random
import time
from threading import Lock 
from typing import Dict, List, Optional, Set, Tuple, Union 

# --- Constants (Ensure these are defined as per your game's requirements) ---
logger = logging.getLogger(__name__) # This will be 'state' or 'game.core.state' depending on import
# To see the CRITICAL logs during startup, ensure your main script's basicConfig level is low enough.
# logger.critical(f"!!!!!!!!!! STATE.PY MODULE LOADED - LOGGER NAME IS: {__name__} !!!!!!!!!!") 

GAME_PHASES = {
    "SETUP": "setup", "JOINING": "joining", "GANGSTER_ASSIGNMENT": "gangster_assignment",
    "WAIT_FOR_AL_CAPONE_CONTINUE": "wait_for_al_capone_continue", "DEALING_CARDS": "dealing_cards",
    "VIEWING": "viewing", "PLAYING": "playing", 
    "CHARACTER_ABILITY_TARGETING": "character_ability_targeting",
    "CHARACTER_ABILITY_ACTION": "character_ability_action",
    "BOTTLE_MATCHING_WINDOW": "bottle_matching_window", "OMERTA_CALLED": "omerta_called",
    "COMPLETED": "completed"
}
PLAYER_STATES = {"ACTIVE": "active", "INACTIVE": "inactive", "SKIPPED_TURN": "skipped_turn"}
GAME_MODES = {"SOLO": "solo", "GROUP": "group"}
JOIN_TIME_LIMIT = 120; JOIN_REMINDER_INTERVAL = 30; CARD_VIEWING_TIME_LIMIT = 30
BOTTLE_MATCH_WINDOW_SECONDS = 5; MIN_PLAYERS = 3; MAX_PLAYERS = 9
INITIAL_CARDS_TO_VIEW = 2; OMERTA_THRESHOLD = 7; OMERTA_PENALTY = 20
SAFE_CARDS_COUNT = 4; HAND_CARDS_COUNT = 4

CHARACTER_CARDS = [ # Ensure this list is complete and correct
    {"name": "The Lady", "points": 15, "ability": "Shuffle an opponent's hand without looking", "ability_time": 20},
    {"name": "The Mole", "points": 15, "ability": "Look at one of your own cards", "ability_time": 15},
    {"name": "The Gangster", "points": 15, "ability": "Swap cards", "ability_time": 30}, # Simplified ability desc.
    {"name": "The Snitch", "points": 20, "ability": "Give deck cards to opponent(s)", "ability_time": 25},
    {"name": "The Driver", "points": 20, "ability": "Discard own Bottle cards", "ability_time": 20},
    {"name": "The Safecracker", "points": 15, "ability": "Interact with the Safe", "ability_time": 20},
    {"name": "The Killer", "points": 15, "ability": "Counter an ability targeting you", "ability_time": 10},
    {"name": "The Witness", "points": 10, "ability": "No special ability", "ability_time": 0},
    {"name": "The Alibi", "points": 0, "ability": "No points", "ability_time": 0},
    {"name": "The Mamma", "points": 15, "ability": "Make opponent skip turn & block Omerta", "ability_time": 20},
    {"name": "Police Patrol", "points": 15, "ability": "Block an opponent's card", "ability_time": 30}
]
FAMOUS_GANGSTERS = ["Al Capone", "Bugsy Siegel", "Lucky Luciano", "Dutch Schultz", "Meyer Lansky", "Pablo Escobar", "John Gotti", "Frank Costello", "Whitey Bulger"]
DEFAULT_GANGSTER_IMAGE = "https://raw.githubusercontent.com/Ola-99/Omerta/refs/heads/main/istockphoto-1437400929-612x612.jpg"
GANGSTER_INFO = {
    "Al Capone": {
        "image": "https://raw.githubusercontent.com/Ola-99/Omerta/refs/heads/main/e8d44d15280e688b830f0ecf98baa68f.jpg",
        "info": "Al Capone rose to become the undisputed crime boss of Chicago during the Prohibition era, notoriously controlling a vast empire of illegal liquor, gambling, and prostitution. His ruthless reputation was solidified by the infamous St. Valentine's Day Massacre in 1929, where seven rival gang members were gunned down, a brutal act widely attributed to his orders. Despite his violent reign, he cleverly evaded conviction for years through bribery and intimidation, once even claiming he was 'just a businessman giving the people what they want.' Ironically, this legendary gangster was ultimately brought down not by murder charges, but by federal charges for income tax evasion, a relatively mundane crime for such a powerful figure.",
        "nickname": "<i><b>Scarface</b></i>"
    },
    "Bugsy Siegel": {
        "image": "https://raw.githubusercontent.com/Ola-99/Omerta/refs/heads/main/MV5BOTM4MWZlYWYtYTVjNy00NzUzLTk3ODYtMzdhZDMzN2Y5N2I5XkEyXkFqcGc%40._V1_FMjpg_UX1000_.jpg",
        "info": "Benjamin Siegel, better known as 'Bugsy' Siegel, rose to prominence as a notorious American gangster, playing a pivotal role in the expansion of organized crime from the East Coast to California. He was famously known for his violent temper and his involvement in 'Murder, Inc.,' the enforcement arm of the National Crime Syndicate. However, Siegel's most enduring legacy is his ambitious vision for Las Vegas, where he spearheaded the development of the Flamingo Hotel and Casino. Despite initial financial struggles and massive cost overruns, this venture helped lay the groundwork for Las Vegas to become the global gambling and entertainment mecca it is today.",
        "nickname": "<i><b>Bugsy</b></i>"
    }, 
    "Lucky Luciano": {
        "image": "https://raw.githubusercontent.com/Ola-99/Omerta/refs/heads/main/Lucky-Luciano.jpg",
        "info": "Charles 'Lucky' Luciano, considered the father of modern organized crime in America, revolutionized the Mafia by establishing The Commission, a governing body that brought order and reduced inter-gang warfare. He was notorious for orchestrating the murders of old-guard bosses in the Castellammarese War, which solidified his power and paved the way for a more unified crime syndicate. His influence extended far beyond racketeering, as he played a surprising role in aiding the Allied efforts during World War II by ensuring the security of New York's docks. Despite being deported to Italy, Luciano continued to exert significant control over the American Mafia from afar, leaving an indelible mark on the criminal underworld.",
        "nickname": "<i><b>Lucky</b></i>"
    },
    "Dutch Schultz": {
        "image": "https://raw.githubusercontent.com/Ola-99/Omerta/refs/heads/main/schultz400.jpg",
        "info": "Dutch Schultz, born Arthur Flegenheimer, rose to infamy during the Prohibition era as a ruthless Jewish-American gangster. He was notorious for his bootlegging operations, which he expanded into illegal gambling and extortion with brutal tactics. His reputation for violence was cemented through acts like kidnapping and torture of rivals, leading to his rise as a major New York crime boss. Ultimately, Schultz's volatile nature and an attempt to assassinate special prosecutor Thomas Dewey led to his own murder by rival mob syndicates in 1935.",
        "nickname": "<i><b>The Beer Baron of the Bronx</b></i> or <i><b>The Dutchman</b></i>"
    },
    "Meyer Lansky": {
        "image": "https://raw.githubusercontent.com/Ola-99/Omerta/refs/heads/main/Colorized_image_of_Meyer_Lansky.jpg",
        "info": "Meyer Lansky was a brilliant and cunning strategist who masterminded the financial operations of the American Mafia for decades. He was notorious for his discreet yet indispensable role in organizing and legitimizing the syndicate's illicit profits through complex money laundering schemes and offshore accounts. Lansky's shrewdness allowed him to build vast wealth from gambling operations in Cuba and Las Vegas, often avoiding the public spotlight and violence associated with his more flamboyant associates. Despite his immense power and influence, he famously died of natural causes at 80 years old, a rare feat for a mob figure of his stature, without ever serving significant prison time for his criminal activities.",
        "nickname": "<i><b>The Mob's Accountant</b></i> or <i><b>Financial Genius</b></i>"
    },
    "Pablo Escobar": {
        "image": "https://raw.githubusercontent.com/Ola-99/Omerta/refs/heads/main/pablo-escobar-the-godfather-of-the-medellin-cartel-in-news-photo-1681421229.jpg",
        "info": "Pablo Escobar, dubbed <b>The King of Cocaine</b>, became one of the wealthiest and most violent criminals in history, controlling an estimated 80% of the world's cocaine trade at his peak. He was notorious for his 'plata o plomo' (silver or lead) policy, offering bribes to officials and ordering assassinations for those who refused. Despite his brutal tactics, he gained a 'Robin Hood' image among many poor Colombians by funding community projects like housing and soccer fields. This stark contrast between his philanthropic gestures and his reign of terror, which included bombings and thousands of murders, solidified his unforgettable and controversial legacy.",
        "nickname": "<i><b>El Patrón</b></i>"
    },
    "John Gotti": {
        "image": "https://raw.githubusercontent.com/Ola-99/Omerta/refs/heads/main/5bb4f696dde86764aa6af2f6.webp",
        "info": "John Gotti, famously known as the 'Dapper Don' and 'Teflon Don,' rose to prominence as the flamboyant boss of the Gambino crime family, captivating the media with his expensive suits and public defiance of law enforcement. He was notorious for orchestrating the brazen murder of his predecessor, Paul Castellano, outside a New York steakhouse, which propelled him to the top of the Mafia hierarchy. Gotti earned his 'Teflon' moniker for repeatedly evading conviction in the 1980s, seemingly untouchable by legal charges despite overwhelming evidence. However, his downfall ultimately came when his own underboss, Sammy 'The Bull' Gravano, became a government witness, leading to Gotti's conviction and life imprisonment, proving even the 'Teflon Don' wasn't invincible.",
        "nickname": "<i><b>The Dapper Don</b></i> or <i><b>The Teflon Don</b></i>"
    },
    "Frank Costello": {
        "image": "https://raw.githubusercontent.com/Ola-99/Omerta/refs/heads/main/8d2e2f303415960e872ca7f2dc12b3cf.jpg",
        "info": "Frank Costello was a highly influential and intellectual mob boss who preferred negotiation and political connections over brute force, distinguishing him from many of his violent contemporaries. He was notorious for skillfully manipulating politicians and judges, earning him immense power and influence far beyond traditional racketeering. Costello famously survived an assassination attempt orchestrated by Vito Genovese, a rival who sought to usurp his power, showcasing his resilience and the constant internal struggles within the Mafia. His public testimonies during the Kefauver hearings in the 1950s, though often evasive, offered a rare glimpse into the hidden world of organized crime and cemented his image as a sophisticated, albeit criminal, power broker.",
        "nickname": "<i><b>The Prime Minister of the Underworld</b></i>"
    },
    "Whitey Bulger": {
        "image": "https://raw.githubusercontent.com/Ola-99/Omerta/refs/heads/main/Mug-shot-Whitey-Bulger-2011%20(1).jpg",
        "info": "James 'Whitey' Bulger, a notoriously ruthless boss of Boston's Irish Mob, gained infamy not only for his violent criminal empire but also for his shocking status as a long-term FBI informant. This 'devil's deal' allowed him to operate with impunity, as corrupt FBI agents protected him while he supplied information on rival Italian-American crime families. He spent 16 years as one of the FBIs Ten Most Wanted Fugitives, often behind only Osama bin Laden, before finally being captured in California in 2011. His story, a bizarre mix of brutal gangsterism and law enforcement complicity, inspired numerous books and films, including the Oscar-winning <i>The Departed</i>.",
        "nickname": "<i><b>Whitey</b></i>"
    }
}

# --- GameState Class ---
class GameState:
    def __init__(self):
        # logger.critical("!!!!!!!!!! GameState INSTANCE CREATED !!!!!!!!!!")
        self.active_games: Dict[int, dict] = {}
        self._lock = Lock() 

    def _internal_get_game_unsafe(self, chat_id: int) -> Optional[dict]:
        """ Internal method: MUST BE CALLED WHEN self._lock is ALREADY HELD. """
        # logger.debug(f"State: _internal_get_game_unsafe - Accessing for chat {chat_id}.")
        return self.active_games.get(chat_id)

    def get_game(self, chat_id: int) -> Optional[dict]:
        """Public method to get game data, acquires lock."""
        # logger.info(f"State: get_game (public) - ENTERED for chat_id {chat_id}.") # Can be noisy
        # logger.debug(f"State: get_game (public) - Attempting lock for chat_id {chat_id}.")
        with self._lock:
            # logger.info(f"State: get_game (public) - Lock acquired for chat_id {chat_id}.")
            result = self._internal_get_game_unsafe(chat_id)
            # logger.info(f"State: get_game (public) - _internal_get_game_unsafe returned {'data' if result else 'None'}.")
            # logger.debug(f"State: get_game (public) - Releasing lock for chat_id {chat_id}.")
            return result

    def add_game(self, chat_id: int, user_id: int, user_first_name: str, user_username: Optional[str]) -> dict:
        logger.debug(f"State: add_game - Attempting lock for chat {chat_id}.")
        with self._lock:
            logger.debug(f"State: add_game - Lock acquired for chat {chat_id}.")
            game_data = {
                'chat_id': chat_id, 'phase': GAME_PHASES["SETUP"], 'mode': None, 'host_id': user_id,
                'players': [], 'ai_players': [], 'deck': [], 'discard_pile': [], 'safe': [],
                'current_player_id': None, 'al_capone_player_id': None, 'turn_order': [], 
                'cycle_count': 0, 'omerta_caller_id': None, 'game_log': [],
                'join_message_id': None, 'join_start_time': None, 'join_end_job_name': None,
                'join_reminder_job_name': None, 'viewing_start_time': None,
                'viewing_timer_job_name': None, 'al_capone_continue_message_id': None,
                'active_ability_context': None, 'bottle_match_context': None, 
                'blocked_cards': {}, 'gangster_assignments': {}, 'player_turn_context': {},
            }
            self.active_games[chat_id] = game_data
            logger.info(f"State: New game shell created for chat {chat_id} by user {user_id}.")
            logger.debug(f"State: add_game - Releasing lock for chat {chat_id}.")
            return game_data

    def add_player_to_game(self, chat_id: int, user_id: int, first_name: str, username: Optional[str]) -> Optional[dict]:
        logger.info(f"State: add_player_to_game - ENTERED for user {user_id}, chat {chat_id}.")
        logger.debug(f"State: add_player_to_game - Attempting lock for chat {chat_id}, user {user_id}.")
        with self._lock:
            logger.info(f"State: add_player_to_game - Lock acquired for chat {chat_id}, user {user_id}.")
            
            logger.debug(f"State: add_player_to_game - Attempting self._internal_get_game_unsafe({chat_id}).")
            game = self._internal_get_game_unsafe(chat_id)
            logger.info(f"State: add_player_to_game - self._internal_get_game_unsafe({chat_id}) returned {'game object' if game else 'None'}.")

            if not game:
                logger.error(f"State: add_player_to_game: Game not found for chat {chat_id} (lock was held).")
                logger.debug(f"State: add_player_to_game - Releasing lock (no game found).")
                return None
            
            logger.debug(f"State: add_player_to_game - Checking MAX_PLAYERS ({MAX_PLAYERS}). Current: {len(game['players']) + len(game['ai_players'])}")
            if len(game['players']) + len(game['ai_players']) >= MAX_PLAYERS:
                logger.warning(f"State: add_player_to_game: Game full for chat {chat_id}. Cannot add player {user_id}.")
                logger.debug(f"State: add_player_to_game - Releasing lock (game full).")
                return None 
            
            logger.debug(f"State: add_player_to_game - Checking if player {user_id} already exists.")
            if any(p['id'] == user_id for p in game['players']):
                logger.info(f"State: add_player_to_game: Player {user_id} already in game.")
                logger.debug(f"State: add_player_to_game - Releasing lock (player exists).")
                return next((p for p in game['players'] if p['id'] == user_id), None)

            player_data = {
                'id': user_id, 'first_name': first_name, 'username': username,
                'hand': [], 'viewed_card_indices': set(), 'viewed_all_initial_cards': False,
                'gangster': None, 'status': PLAYER_STATES["ACTIVE"], 'join_time': time.time(),
                'is_ai': False, 'score_this_round': 0, 'cannot_call_omerta': False,
                'viewing_message_id': None
            }
            game['players'].append(player_data)
            logger.info(f"State: Successfully added human player {user_id} ({first_name}) to game in chat {chat_id}.")
            logger.debug(f"State: add_player_to_game - Releasing lock.")
            return player_data

    def add_ai_player_to_game(self, chat_id: int, ai_name_prefix: str = "AI") -> Optional[dict]:
        logger.info(f"State: add_ai_player_to_game - ENTERED for chat {chat_id}, prefix {ai_name_prefix}.")
        logger.debug(f"State: add_ai_player_to_game - Attempting lock for chat {chat_id}.")
        with self._lock:
            logger.info(f"State: add_ai_player_to_game - Lock acquired for chat {chat_id}.")

            logger.debug(f"State: add_ai_player_to_game - Attempting self._internal_get_game_unsafe({chat_id}).")
            game = self._internal_get_game_unsafe(chat_id)
            logger.info(f"State: add_ai_player_to_game - self._internal_get_game_unsafe({chat_id}) returned {'game object' if game else 'None'}.")

            if not game:
                logger.error(f"State: add_ai_player_to_game: Game not found for chat {chat_id} (lock was held).")
                logger.debug(f"State: add_ai_player_to_game - Releasing lock (no game found).")
                return None
            
            logger.debug(f"State: add_ai_player_to_game - Checking MAX_PLAYERS ({MAX_PLAYERS}). Current: {len(game['players']) + len(game['ai_players'])}")
            if len(game['players']) + len(game['ai_players']) >= MAX_PLAYERS:
                 logger.warning(f"State: add_ai_player_to_game: Game full for chat {chat_id}. Cannot add AI.")
                 logger.debug(f"State: add_ai_player_to_game - Releasing lock (game full).")
                 return None

            ai_id = f"ai_{len(game['ai_players']) + 1}_{random.randint(1000,9999)}"
            name_to_use = f"{ai_name_prefix} Bot {len(game['ai_players']) + 1}"
            ai_player_data = {
                'id': ai_id, 'first_name': name_to_use, 'username': None,
                'hand': [], 'viewed_card_indices': set(), 'viewed_all_initial_cards': False,
                'gangster': None, 'status': PLAYER_STATES["ACTIVE"], 'join_time': time.time(),
                'is_ai': True, 'score_this_round': 0, 'cannot_call_omerta': False
            }
            game['ai_players'].append(ai_player_data)
            logger.info(f"State: Successfully added AI player ({ai_id}, {name_to_use}) to game in chat {chat_id}.")
            logger.debug(f"State: add_ai_player_to_game - Releasing lock.")
            return ai_player_data
    
    def end_game(self, chat_id: int):
        logger.debug(f"State: end_game - Attempting lock for chat {chat_id}.")
        with self._lock:
            logger.debug(f"State: end_game - Lock acquired for chat {chat_id}.")
            if chat_id in self.active_games:
                del self.active_games[chat_id]
                logger.info(f"State: Game in chat {chat_id} ended and removed.")
            else:
                logger.info(f"State: No active game found in chat {chat_id} to end.")
            logger.debug(f"State: end_game - Releasing lock.")

    @staticmethod
    def create_deck() -> list:
        deck = []
        for value in range(1, 11): 
            for _ in range(4): 
                deck.append({"type": "bottle", "value": value, "name": f"Bottle {value}", "points": value})
        for char_template in CHARACTER_CARDS:
            count = 3 if char_template["name"] in ["The Alibi", "The Witness"] else 2
            for _ in range(count):
                deck.append({
                    "type": "character", "name": char_template["name"], 
                    "points": char_template["points"], "ability": char_template["ability"], 
                    "ability_time": char_template["ability_time"]
                })
        random.shuffle(deck)
        logger.debug(f"State: New deck created with {len(deck)} cards.")
        return deck

    def assign_gangsters_to_players(self, chat_id: int) -> bool:
        logger.debug(f"State: assign_gangsters_to_players - Attempting lock for chat {chat_id}.")
        with self._lock:
            logger.debug(f"State: assign_gangsters_to_players - Lock acquired for chat {chat_id}.")
            game = self._internal_get_game_unsafe(chat_id)
            if not game: 
                logger.error(f"S: assign_gangsters: Game not found for {chat_id}.")
                logger.debug(f"State: assign_gangsters_to_players - Releasing lock (game not found).")
                return False
            
            all_participants = game.get('players', []) + game.get('ai_players', [])
            if not all_participants: 
                logger.warning(f"S: assign_gangsters: No participants in {chat_id} for assignment.")
                logger.debug(f"State: assign_gangsters_to_players - Releasing lock (no participants).")
                return False

            num_participants = len(all_participants)
            
            # Prepare gangster pool
            available_gangsters = FAMOUS_GANGSTERS[:] # Make a copy
            if "Al Capone" not in available_gangsters and FAMOUS_GANGSTERS: # Should not happen if const is correct
                logger.error("CRITICAL: Al Capone is missing from FAMOUS_GANGSTERS constant!")
                # Fallback: add him to ensure game can run
                available_gangsters.append("Al Capone")
            
            random.shuffle(available_gangsters)

            assigned_gangsters_list = []
            # Ensure Al Capone is included if there are enough unique gangsters for participants
            if num_participants > 0:
                if "Al Capone" in available_gangsters[:num_participants]:
                    # Al Capone is already in the top N shuffled gangsters, good.
                    assigned_gangsters_list = available_gangsters[:num_participants]
                else:
                    # Al Capone is not in the top N. We need to force him in.
                    # Remove Al Capone from wherever he is in the shuffled list (if present beyond N)
                    if "Al Capone" in available_gangsters:
                        available_gangsters.remove("Al Capone")
                    
                    # Take N-1 gangsters from the shuffled list (which no longer contains Al Capone)
                    assigned_gangsters_list = available_gangsters[:max(0, num_participants - 1)]
                    # Add Al Capone to this list
                    assigned_gangsters_list.append("Al Capone")
                    # Shuffle again to make Al Capone's position random among the assigned
                    random.shuffle(assigned_gangsters_list) 
            
            # If not enough unique gangsters from FAMOUS_GANGSTERS for all participants (after ensuring Al Capone)
            # fill remaining spots with "Rookie"
            while len(assigned_gangsters_list) < num_participants:
                assigned_gangsters_list.append(f"Rookie {len(assigned_gangsters_list) - num_participants + 1 + (num_participants - len(FAMOUS_GANGSTERS) if FAMOUS_GANGSTERS else 0 )}")


            game['al_capone_player_id'] = None # Reset before assignment
            # Shuffle participants to make the assignment order random too
            random.shuffle(all_participants) 

            for i, participant_obj in enumerate(all_participants):
                if i < len(assigned_gangsters_list):
                    gangster_name = assigned_gangsters_list[i]
                    participant_obj['gangster'] = gangster_name
                    game['gangster_assignments'][participant_obj['id']] = gangster_name
                    if gangster_name == "Al Capone":
                        game['al_capone_player_id'] = participant_obj['id']
                else: # Should not be reached if logic above is correct
                    logger.warning(f"S: Ran out of gangsters to assign in chat {chat_id}. Assigning default.")
                    participant_obj['gangster'] = f"Default Gangster {i}"
                    game['gangster_assignments'][participant_obj['id']] = f"Default Gangster {i}"

            # Final check: If Al Capone somehow wasn't assigned (e.g., num_participants = 0 edge case missed, or list logic error)
            # and there are participants, force assign to the first one.
            if not game.get('al_capone_player_id') and all_participants:
                logger.warning(f"S: Al Capone fallback assignment triggered for chat {chat_id}.")
                # Clear any other Al Capone assignments if any by error
                for p_id, g_name in list(game['gangster_assignments'].items()):
                    if g_name == "Al Capone": game['gangster_assignments'][p_id] = "Rookie Reassigned" # Demote
                
                first_participant = all_participants[0] # Already shuffled, so this is random enough
                first_participant['gangster'] = "Al Capone"
                game['gangster_assignments'][first_participant['id']] = "Al Capone"
                game['al_capone_player_id'] = first_participant['id']
            
            logger.info(f"State: Gangsters assigned in {chat_id}. Al Capone is player ID: {game.get('al_capone_player_id')}. Assignments: {game['gangster_assignments']}")
            logger.debug(f"State: assign_gangsters_to_players - Releasing lock.")
            return True

    def deal_cards_to_players(self, chat_id: int) -> bool:
        logger.debug(f"State: deal_cards_to_players - Attempting lock for {chat_id}.")
        with self._lock:
            logger.debug(f"State: deal_cards_to_players - Lock acquired.")
            game = self._internal_get_game_unsafe(chat_id)
            if not game: logger.error(f"S: deal_cards: Game not found for {chat_id}."); return False
            
            game['deck'] = self.create_deck()
            all_participants = game['players'] + game['ai_players']
            if not all_participants: logger.warning(f"S: No participants to deal to in {chat_id}."); return False

            for p in all_participants: p['hand'] = [game['deck'].pop() for _ in range(HAND_CARDS_COUNT)] if len(game['deck']) >= HAND_CARDS_COUNT else []
            game['safe'] = [game['deck'].pop() for _ in range(SAFE_CARDS_COUNT)] if len(game['deck']) >= SAFE_CARDS_COUNT else []
            game['discard_pile'] = []; game['cycle_count'] = 0

            ac_id = game.get('al_capone_player_id')
            if ac_id:
                # Use internal unsafe get player since lock is held
                al_capone_player = self._internal_get_player_by_id_unsafe(game, ac_id) 
                other_players = sorted([p for p in all_participants if p['id'] != ac_id], key=lambda p: p.get('join_time', 0))
                game['turn_order'] = ([al_capone_player] if al_capone_player else []) + other_players
            else: 
                logger.error(f"S: Al Capone ID missing for turn order in {chat_id}!")
                game['turn_order'] = sorted(all_participants, key=lambda p: p.get('join_time', 0))
            
            game['turn_order'] = [p for p in game['turn_order'] if p] 
            if game['turn_order']: game['current_player_id'] = game['turn_order'][0]['id']
            else: logger.error(f"S: Turn order empty after dealing in {chat_id}!"); return False
            
            logger.info(f"S: Cards dealt in {chat_id}. Current: {game['current_player_id']}. Safe: {len(game['safe'])}.")
            logger.debug(f"State: deal_cards_to_players - Releasing lock.")
            return True

    def _internal_get_player_by_id_unsafe(self, game_data: dict, player_id: Union[int, str]) -> Optional[dict]:
        """ Internal method: MUST BE CALLED WHEN self._lock is ALREADY HELD and game_data is passed. """
        if not game_data: return None
        for p_list_key in ['players', 'ai_players']:
            for p in game_data.get(p_list_key, []):
                if p['id'] == player_id: return p
        return None

    def get_player_by_id(self, chat_id: int, player_id: Union[int, str]) -> Optional[dict]:
        # Public version that acquires lock and gets game first
        logger.debug(f"State: get_player_by_id (public) - ENTERED for player {player_id} in chat {chat_id}.")
        game = self.get_game(chat_id) # This handles locking
        if not game: 
            logger.warning(f"State: get_player_by_id - Game not found for chat {chat_id} when searching for player {player_id}.")
            return None
        # Now call the unsafe version since we have 'game' (which is a copy of the dict from active_games if get_game returns a copy,
        # or a direct reference if not. If it's a reference, modifying it modifies the original, so lock is implicitly managed by caller of get_player_by_id sometimes)
        # For safety here, let's iterate on the 'game' object fetched by the public get_game.
        # The lock from public get_game is released after it returns 'game'.
        # This means get_player_by_id, if it needs to be super safe about modifications to game *during* its search,
        # would need its own lock or ensure 'game' is a deepcopy.
        # Given current usage, iterating the returned 'game' dict for read-only is fine.
        for p_list_key in ['players', 'ai_players']:
            for p in game.get(p_list_key, []):
                if p['id'] == player_id:
                    logger.debug(f"State: get_player_by_id - Found player {player_id} in chat {chat_id}.")
                    return p
        logger.warning(f"State: get_player_by_id - Player {player_id} not found in chat {chat_id}.")
        return None

    def calculate_score_for_hand(self, hand: List[dict]) -> int:
        if not hand: return 0
        return sum(card.get('points', card.get('value', 0)) for card in hand if card)

    def get_active_players_in_turn_order(self, chat_id: int) -> List[dict]:
        game = self.get_game(chat_id) 
        if not game or not game.get('turn_order'): return []
        # Ensure players in turn_order still exist and are active by cross-referencing main player lists
        active_ids_in_game = {p['id'] for p in game.get('players', []) + game.get('ai_players', []) if p.get('status') == PLAYER_STATES["ACTIVE"]}
        return [p for p in game['turn_order'] if p.get('id') in active_ids_in_game and p.get('status') == PLAYER_STATES["ACTIVE"]]