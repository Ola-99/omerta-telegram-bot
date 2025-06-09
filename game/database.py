import sqlite3
import os
import logging
from threading import Lock
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: str = "omerta_stats.db"):
        self.db_path = os.path.abspath(db_path)
        self.conn = None
        self.lock = Lock()
        self.initialize_db()

    def initialize_db(self):
        """Initialize database connection and create tables"""
        with self.lock:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = self.conn.cursor()
            
            # Player stats table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_stats (
                    player_id INTEGER,
                    chat_id INTEGER,
                    player_name TEXT,
                    games_played INTEGER DEFAULT 0,
                    games_won INTEGER DEFAULT 0,
                    total_score INTEGER DEFAULT 0,
                    avg_score REAL DEFAULT 0,
                    PRIMARY KEY (player_id, chat_id)
                )''')
            
            # Game history table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS game_history (
                    game_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    winner_id INTEGER,
                    winner_name TEXT,
                    game_score INTEGER,
                    game_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
            
            # AI players table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ai_players (
                    ai_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    ai_name TEXT,
                    games_played INTEGER DEFAULT 0,
                    games_won INTEGER DEFAULT 0,
                    total_score INTEGER DEFAULT 0,
                    avg_score REAL DEFAULT 0
                )''')
            
            self.conn.commit()

    def execute(self, query: str, params: tuple = None) -> Optional[List[Tuple]]:
        """Generic SQL execution method with error handling"""
        try:
            with self.lock:
                cursor = self.conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                self.conn.commit()
                
                if query.strip().upper().startswith("SELECT"):
                    return cursor.fetchall()
                return None # Explicitly return None for non-SELECT queries if needed elsewhere
        except sqlite3.Error as e:
            logger.error(f"Database error: {e} for query: {query} with params: {params}")
            if self.conn: # Ensure conn exists before rollback
                self.conn.rollback()
            raise
        except sqlite3.OperationalError as e: # Specific handling for operational errors
            logger.error(f"Database operational error: {e} for query: {query} with params: {params}")
            if "database is locked" in str(e).lower():
                logger.warning("Database locked - consider retry logic or adjusting transaction scope.")
            elif "closed" in str(e).lower() or not self.conn: # Check if connection is None
                logger.warning("Database connection closed or uninitialized. Re-initializing.")
                self.initialize_db()  # Reconnect
                return self.execute(query, params) # Retry after re-initialization
            raise # Re-raise if not handled

    def update_player_stats(self, game_data: dict): # This function implies game_data has specific structure
        """Update stats after a game ends"""
        # This function needs to be adapted to how scores are stored in your final game_data
        # Assuming game_data['final_scores_list'] = [{'id': player_id, 'name': name, 'score': score, 'is_winner': bool}]
        chat_id = game_data.get("chat_id")
        if not chat_id or not game_data.get("final_scores_list"):
            logger.error("Cannot update player stats: chat_id or final_scores_list missing from game_data.")
            return

        for player_stat in game_data["final_scores_list"]:
            player_id = player_stat.get("id")
            name = player_stat.get("name", "Unknown Player")
            score = player_stat.get("score", 0)
            is_winner = player_stat.get("is_winner", False)
            is_ai = player_stat.get("is_ai", False) # Need to know if it's an AI or human

            if is_ai:
                # Update ai_players table (schema might need adjustment if using string IDs for AI)
                # Assuming ai_id is an integer for DB primary key, but your game uses string like "ai_1"
                # This part needs alignment between game AI ID format and DB schema.
                # For now, skipping AI stat updates or assuming a mapping exists.
                logger.info(f"Skipping AI player stat update for {name} (feature to align IDs/schema).")
                continue

            if not player_id: continue

            # Using a more robust way to update, handling potential NULLs from COALESCE better.
            # Check if player exists
            existing_stats = self.execute(
                "SELECT games_played, games_won, total_score FROM player_stats WHERE player_id=? AND chat_id=?",
                (player_id, chat_id)
            )
            
            if existing_stats:
                games_played, games_won, total_score = existing_stats[0]
                new_games_played = games_played + 1
                new_games_won = games_won + (1 if is_winner else 0)
                new_total_score = total_score + score
                self.execute(
                    '''UPDATE player_stats 
                       SET player_name=?, games_played=?, games_won=?, total_score=?
                       WHERE player_id=? AND chat_id=?''',
                    (name, new_games_played, new_games_won, new_total_score, player_id, chat_id)
                )
            else: # New player
                self.execute(
                    '''INSERT INTO player_stats 
                       (player_id, chat_id, player_name, games_played, games_won, total_score)
                       VALUES (?, ?, ?, ?, ?, ?)''',
                    (player_id, chat_id, name, 1, (1 if is_winner else 0), score)
                )
            logger.info(f"Updated stats for player {player_id} in chat {chat_id}.")

    def get_player_stats(self, player_id: int, chat_id: int) -> Optional[dict]:
        """Get individual player stats"""
        # Ensure player_id is treated as integer for DB if that's the schema
        result = self.execute(
            "SELECT player_id, chat_id, player_name, games_played, games_won, total_score FROM player_stats WHERE player_id=? AND chat_id=?", # Explicitly list columns
            (int(player_id), chat_id) # Cast player_id if it might come as string
        )
        return self._row_to_dict(result[0], ["player_id", "chat_id", "player_name", "games_played", "games_won", "total_score"]) if result else None


    def get_leaderboard(self, chat_id: int, limit: int = 5) -> List[dict]: # Simplified, assumes wins then score
        """Get leaderboard for a chat, ordered by wins (desc) then total_score (asc)."""
        query = """
            SELECT player_id, chat_id, player_name, games_played, games_won, total_score 
            FROM player_stats 
            WHERE chat_id = ?
            ORDER BY games_won DESC, total_score ASC 
            LIMIT ? 
        """ # Explicitly list columns
        results = self.execute(query, (chat_id, limit))
        columns = ["player_id", "chat_id", "player_name", "games_played", "games_won", "total_score"]
        return [self._row_to_dict(row, columns) for row in results] if results else []

    def _row_to_dict(self, row: Optional[Tuple], column_names: List[str]) -> Optional[dict]:
        """Convert SQL row to dictionary given column names."""
        if not row:
            return None
        return dict(zip(column_names, row))

    def __del__(self):
        """Cleanup when instance is destroyed"""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Database connection closed.")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")