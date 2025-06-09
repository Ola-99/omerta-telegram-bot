import os
import logging
import random
import asyncio
import time
import copy 
from dotenv import load_dotenv
from typing import Optional, Union, List, Dict, Set, Tuple 

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes, JobQueue
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram import InputMediaPhoto

from game.core.state import (
    GameState, GAME_PHASES, PLAYER_STATES, GAME_MODES,
    MIN_PLAYERS, MAX_PLAYERS, JOIN_TIME_LIMIT, JOIN_REMINDER_INTERVAL,
    CARD_VIEWING_TIME_LIMIT, BOTTLE_MATCH_WINDOW_SECONDS,
    INITIAL_CARDS_TO_VIEW, OMERTA_THRESHOLD, OMERTA_PENALTY,
    CHARACTER_CARDS, FAMOUS_GANGSTERS, GANGSTER_INFO, DEFAULT_GANGSTER_IMAGE,
    SAFE_CARDS_COUNT, HAND_CARDS_COUNT
)
from bot import keyboards 
from game.database import Database

logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(lineno)d - %(message)s' 
)
logger = logging.getLogger(__name__)
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    logger.critical("CRITICAL: BOT_TOKEN environment variable not found!")
    exit()
game_state_manager = GameState()

db = Database()

SETUP_IMAGE_URL = "https://raw.githubusercontent.com/Ola-99/Omerta/refs/heads/main/Gangsters%20playing%20cards.png" 

# --- Helper Functions ---
def escape_html(text: Optional[str]) -> str:
    if not text: return ''
    return str(text).replace('&', '&').replace('<', '<').replace('>', '>').replace('"', '"').replace("'", '&#39;')

def get_player_mention(player_data: Optional[dict]) -> str:
    if not player_data: return "<i>Unknown Player</i>"
    player_id = player_data.get('id')
    name = escape_html(player_data.get('first_name', 'Player'))
    if player_data.get('is_ai'): return f"🤖 {name}"
    try: 
        tg_id = int(player_id)
        if player_data.get('username'): return f"<a href='tg://user?id={tg_id}'>@{escape_html(player_data['username'])}</a>"
        return f"<a href='tg://user?id={tg_id}'>{name}</a>"
    except (ValueError, TypeError): 
        if player_data.get('username'): return f"@{escape_html(player_data['username'])}"
        return name

def format_player_list_html(game: dict) -> str:
    items = [get_player_mention(p_data) for p_data in game.get('players', []) + game.get('ai_players', [])]
    return "\n".join(items) if items else "No players yet."

async def send_message_to_player(context: ContextTypes.DEFAULT_TYPE, player_id: Union[int, str], text: str, reply_markup=None, parse_mode=ParseMode.HTML) -> Optional[int]:
    if isinstance(player_id, str) and player_id.startswith("ai_"):
        logger.debug(f"Skipping PM to AI {player_id}. Message: {text[:70]}...")
        return None
    try:
        message_chat_id = player_id
        if isinstance(player_id, str) and str(player_id).isdigit():
            message_chat_id = int(player_id)
        message = await context.bot.send_message(chat_id=message_chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        return message.message_id
    except TelegramError as e:
        logger.error(f"TelegramError sending PM to player {player_id}: {e} (Message: {text[:70]}...)")
        if any(err_msg in str(e).lower() for err_msg in ["bot was blocked", "user is deactivated", "chat not found", "bot can't initiate conversation"]):
            logger.warning(f"Player {player_id} is unreachable or has not initiated chat with bot.")
        return None
    except Exception as e:
        logger.error(f"Generic error sending PM to player {player_id}: {e} (Message: {text[:70]}...)")
        return None

def cancel_job(context: ContextTypes.DEFAULT_TYPE, job_name: Optional[str]) -> bool:
    if job_name:
        jobs = context.job_queue.get_jobs_by_name(job_name)
        if jobs:
            for job in jobs: job.schedule_removal(); logger.info(f"Cancelled job: {job.name}")
            return True
        else: logger.debug(f"Job {job_name} not found for cancellation.")
    return False

async def custom_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
    chat_to_inform = None
    if isinstance(update, Update):
        if update.effective_chat: chat_to_inform = update.effective_chat.id
        elif update.callback_query and update.callback_query.message and update.callback_query.message.chat:
            chat_to_inform = update.callback_query.message.chat.id
    if chat_to_inform:
        try: await context.bot.send_message(chat_id=chat_to_inform, text="😕 Oops! An unexpected error occurred. If the game is stuck, try /endgame and /newgame.")
        except TelegramError as e: logger.error(f"Failed to send error message to chat {chat_to_inform}: {e}")

# --- Callback Query Handler ---
async def send_join_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    job_data = context.job.data; chat_id = job_data['chat_id']
    game = game_state_manager.get_game(chat_id)
    if not game or game['phase'] != GAME_PHASES["JOINING"]:
        if context.job: context.job.schedule_removal(); logger.info(f"Join reminder job for {chat_id} stopped (invalid state).")
        return
    time_elapsed = time.time() - game.get('join_start_time', time.time())
    time_left = int(JOIN_TIME_LIMIT - time_elapsed)
    if time_left <= 0:
        if context.job: context.job.schedule_removal(); logger.info(f"Join reminder for {chat_id} time_left <=0.")
        return
    if game.get('join_message_id'):
        num_players = len(game.get('players',[])) + len(game.get('ai_players',[]))
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=game['join_message_id'],
                text=(f"⏳ Reminder: Lobby open! Join in ~{max(0,time_left)}s.\n<b>Players ({num_players}/{MAX_PLAYERS}):</b>\n{format_player_list_html(game)}"),
                reply_markup=keyboards.get_join_game_keyboard() if num_players < MAX_PLAYERS else None,
                parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.warning(f"Failed join reminder for {chat_id}: {e}.")
            if "message to edit not found" in str(e).lower() and context.job: context.job.schedule_removal()

async def _update_join_message(context: ContextTypes.DEFAULT_TYPE, game_chat_id: int, game: dict):
    """Helper to update the main join message or send a new one if ID is lost."""
    if not game or not game.get('join_message_id'):
        logger.warning(f"_update_join_message: No game or join_message_id for chat {game_chat_id}")
        return

    num_total_players = len(game.get('players', [])) + len(game.get('ai_players', []))
    time_left_approx = int(JOIN_TIME_LIMIT - (time.time() - game.get('join_start_time', time.time())))
    player_list_str = format_player_list_html(game)
    
    join_msg_txt = (f"👥 Group Game lobby open! Join within ~{max(0, time_left_approx)}s.\n\n"
                    f"<b>Players ({num_total_players}/{MAX_PLAYERS}):</b>\n{player_list_str}")
    
    new_join_keyboard = keyboards.get_join_game_keyboard(game, MAX_PLAYERS) if num_total_players < MAX_PLAYERS else None
    
    try:
        await context.bot.edit_message_text(
            chat_id=game_chat_id,
            message_id=game['join_message_id'],
            text=join_msg_txt,
            reply_markup=new_join_keyboard,
            parse_mode=ParseMode.HTML
        )
        logger.debug(f"Join message updated for chat {game_chat_id}")
    except TelegramError as e:
        logger.error(f"Error updating join message for chat {game_chat_id}: {e}")
        if "message to edit not found" in str(e).lower():
            game['join_message_id'] = None 

async def join_period_ended_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    job_data = context.job.data; chat_id = job_data['chat_id']
    expected_job_name = job_data.get('expected_job_name')
    game = game_state_manager.get_game(chat_id)
    if game and expected_job_name and game.get('join_end_job_name') != expected_job_name:
        logger.info(f"Stale join_period_ended_job for {chat_id}. Ignoring.")
        return
    if game: game['join_end_job_name'] = None
    await process_join_period_end(chat_id, context)

async def clear_temp_card_view_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    pm_chat_id = job_data.get('pm_chat_id'); message_id = job_data.get('message_id')
    game_chat_id = job_data.get('game_chat_id'); player_id = job_data.get('player_id')
    original_instruction_text = job_data.get('original_instruction_text', "View your cards.")
    expected_job_name = job_data.get('expected_job_name') # Get expected name

    if not all([pm_chat_id, message_id, game_chat_id, player_id]): #... error log ...
        return

    game = game_state_manager.get_game(game_chat_id)
    if not game: #... error log ...
         return
    player_data = game_state_manager.get_player_by_id(game_chat_id, player_id)
    if not player_data: #... error log ...
         return

    # Check if this job is still the active clear job for this player's viewing message
    if expected_job_name and player_data.get('viewing_clear_job_name') != expected_job_name:
        logger.debug(f"Stale clear_temp_card_view_job ({expected_job_name}) for player {player_id}. Current job: {player_data.get('viewing_clear_job_name')}. Ignoring.")
        return
    player_data['viewing_clear_job_name'] = None

async def viewing_timeout_job(context: ContextTypes.DEFAULT_TYPE) -> None: # DEFINITION OF THE MISSING FUNCTION
    job_data = context.job.data; chat_id = job_data['chat_id']
    expected_job_name = job_data.get('expected_viewing_timer_job_name')
    game = game_state_manager.get_game(chat_id)
    
    if not game or game['phase'] != GAME_PHASES["VIEWING"] or \
       (expected_job_name and game.get('viewing_timer_job_name') != expected_job_name and game.get('viewing_timer_job_name') is not None) :
        logger.info(f"viewing_timeout_job (30s) for {chat_id}: Stale or invalid. Aborting."); return
    
    if game: game['viewing_timer_job_name'] = None # This job (30s initial) is done.
    logger.info(f"Initial {CARD_VIEWING_TIME_LIMIT}s viewing time up for chat {chat_id}.")

    all_humans_viewed_this_round = True
    pending_players_mentions = []
    for p_data in game.get('players', []):
        if p_data.get('status') == PLAYER_STATES["ACTIVE"] and not p_data.get('viewed_all_initial_cards'):
            all_humans_viewed_this_round = False
            await send_message_to_player(context, p_data['id'], f"⏳ Reminder! Please view your {INITIAL_CARDS_TO_VIEW} cards. You have 30 more seconds before being marked inactive.")
            pending_players_mentions.append(get_player_mention(p_data))

    if all_humans_viewed_this_round:
        logger.info(f"All humans viewed cards by 30s timeout for {chat_id}. Finalizing.")
        await finalize_viewing_phase_and_start_game(chat_id, context)
    elif pending_players_mentions:
        try:
            await context.bot.send_message(chat_id, f"Still waiting for {', '.join(pending_players_mentions)} to view their cards. They have 30 more seconds.", parse_mode=ParseMode.HTML)
        except TelegramError as e: logger.error(f"Error sending 30s viewing reminder to group: {e}")
        
        final_job_name = f"final_viewing_timeout_{chat_id}_{int(time.time())}"
        game['final_viewing_job_name'] = final_job_name
        logger.info(f"Scheduling final_viewing_warning_timeout_job: {final_job_name} for {chat_id}.")
        context.job_queue.run_once(final_viewing_warning_timeout_job, CARD_VIEWING_TIME_LIMIT, # Another 30s
                                   data={'chat_id': chat_id, 'expected_job_name': final_job_name}, 
                                   name=final_job_name)
    else: # No pending players, but not all viewed (e.g. all became inactive before viewing)
        logger.info(f"30s timeout in {chat_id}: No humans pending viewing, but not all completed. Finalizing.")
        await finalize_viewing_phase_and_start_game(chat_id, context)

async def finalize_viewing_phase_and_start_game(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    game = game_state_manager.get_game(chat_id)
    if not game:
        logger.error(f"FVPaSG: Game not found for chat {chat_id}. Aborting.")
        return
    
    # If already moved to playing or completed, don't re-process
    if game['phase'] not in [GAME_PHASES["VIEWING"], GAME_PHASES["DEALING_CARDS"]]: 
        logger.info(f"FVPaSG: Game for chat {chat_id} already past viewing (Phase: {game['phase']}). Skipping.")
        return
    
    logger.info(f"FVPaSG: Finalizing for chat {chat_id}. Current phase: {game['phase']}")
    game['phase'] = GAME_PHASES["PLAYING"]
    # Cancel any lingering viewing jobs
    cancel_job(context, game.pop('viewing_timer_job_name', None))
    cancel_job(context, game.pop('final_viewing_job_name', None))

    # AI players viewing (ensure this is done if not already)
    for ai_player in game.get('ai_players', []): 
        if not ai_player.get('viewed_all_initial_cards'): 
            ai_player['viewed_card_indices'] = set(random.sample(range(len(ai_player['hand'])), min(len(ai_player['hand']), INITIAL_CARDS_TO_VIEW))) if ai_player.get('hand') else set()
            ai_player['viewed_all_initial_cards'] = True
            logger.debug(f"AI {ai_player['id']} marked as viewed cards during finalize.")

    viewing_summary_parts = ["<b>Card Viewing Summary:</b>"]
    active_player_count = 0
    all_participants_for_summary = game.get('players', []) + game.get('ai_players', [])

    for p_data in all_participants_for_summary:
        if p_data.get('status') == PLAYER_STATES["INACTIVE"]:
            viewing_summary_parts.append(f"{get_player_mention(p_data)} is inactive.")
        elif p_data.get('viewed_all_initial_cards'):
            card_indices_str = ", ".join([f"#{idx+1}" for idx in sorted(list(p_data.get('viewed_card_indices', set())))])
            viewing_summary_parts.append(f"{get_player_mention(p_data)} viewed: {card_indices_str}{' (AI)' if p_data.get('is_ai') else ''}")
            active_player_count +=1
        else: 
            logger.warning(f"Player {p_data['id']} is active but 'viewed_all_initial_cards' is false at finalize. Marking inactive.")
            p_data['status'] = PLAYER_STATES["INACTIVE"]
            viewing_summary_parts.append(f"{get_player_mention(p_data)} failed to confirm viewing and is inactive.")

    try: await context.bot.send_message(chat_id=chat_id, text="\n".join(viewing_summary_parts), parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"FVPaSG: Failed to send summary: {e}")
    
    game['turn_order'] = [p for p in game.get('turn_order', []) if p.get('status') == PLAYER_STATES["ACTIVE"]]
    game['players'] = [p for p in game.get('players', []) if p.get('status') == PLAYER_STATES["ACTIVE"]]
    game['ai_players'] = [p for p in game.get('ai_players', []) if p.get('status') == PLAYER_STATES["ACTIVE"]]
    
    final_active_player_count = len(game['players']) + len(game['ai_players'])

    if final_active_player_count < MIN_PLAYERS: 
        logger.info(f"FVPaSG: Not enough active players ({final_active_player_count}) after viewing in {chat_id}. Ending game.")
        try: await context.bot.send_message(chat_id, "Not enough active players. Game ended. Try /newgame.")
        except TelegramError as e: logger.error(f"FVPaSG: Failed to send game end msg: {e}")
        game_state_manager.end_game(chat_id); return
    
    game['phase'] = GAME_PHASES["PLAYING"]
    logger.info(f"FVPaSG: Phase PLAYING for {chat_id}. Starting first turn.")
    try: await context.bot.send_message(chat_id, "All active players completed card viewing. The game begins now!", parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"FVPaSG: Failed to send game begins msg: {e}")
    await _start_turn_for_current_player(game, context)

async def check_all_active_humans_done_viewing_and_proceed(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    game = game_state_manager.get_game(chat_id)
    if not game or game['phase'] != GAME_PHASES["VIEWING"]: return

    all_done = True
    for p_data in game.get('players', []): # Only check human players who are still active
        if p_data.get('status') == PLAYER_STATES["ACTIVE"] and not p_data.get('viewed_all_initial_cards'):
            all_done = False
            break
    
    if all_done:
        logger.info(f"All active human players in chat {chat_id} have now viewed their cards.")
        await finalize_viewing_phase_and_start_game(chat_id, context)

async def final_viewing_warning_timeout_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    job_data = context.job.data; chat_id = job_data['chat_id']
    expected_job_name = job_data.get('expected_job_name')
    game = game_state_manager.get_game(chat_id)

    if not game or game['phase'] != GAME_PHASES["VIEWING"] or \
       (expected_job_name and game.get('final_viewing_job_name') != expected_job_name and game.get('final_viewing_job_name') is not None):
        logger.info(f"final_viewing_warning_timeout_job for {chat_id}: Stale or invalid. Aborting."); return

    if game: game['final_viewing_job_name'] = None
    logger.info(f"Total 1 minute viewing time up for chat {chat_id}.")
    
    removed_players_mentions = []
    for p_data in game.get('players', []):
        if p_data.get('status') == PLAYER_STATES["ACTIVE"] and not p_data.get('viewed_all_initial_cards'):
            p_data['status'] = PLAYER_STATES["INACTIVE"]
            logger.info(f"Player {p_data['id']} marked INACTIVE for not viewing cards within 1 minute.")
            await send_message_to_player(context, p_data['id'], "You did not view your cards within the total 1-minute grace period and have been removed from this round.")
            removed_players_mentions.append(get_player_mention(p_data))
            # Clean up their viewing PM if it exists
            if p_data.get('viewing_message_id'):
                try: await context.bot.edit_message_text(chat_id=p_data['id'], message_id=p_data['viewing_message_id'], text="Card viewing period ended.", reply_markup=None)
                except TelegramError: pass
                
    if removed_players_mentions:
        try: await context.bot.send_message(chat_id, f"{', '.join(removed_players_mentions)} did not view cards in time and are out of this round.", parse_mode=ParseMode.HTML)
        except TelegramError as e: logger.error(f"Error sending group removal message: {e}")
        
    await finalize_viewing_phase_and_start_game(chat_id, context)

async def ai_attempt_bottle_match_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data; chat_id = job_data['chat_id']; ai_player_id = job_data['ai_player_id']
    ai_card_idx_to_match = job_data['ai_card_idx_to_match']; expected_bottle_ctx_signature = job_data['expected_bottle_ctx_signature']
    
    game = game_state_manager.get_game(chat_id)
    if not game or game['phase'] != GAME_PHASES["BOTTLE_MATCHING_WINDOW"]:
        logger.info(f"AI BottleMatchJob {ai_player_id} in C:{chat_id}: Window closed/phase changed. Aborting."); return
    
    current_bottle_ctx = game.get('bottle_match_context')
    if not current_bottle_ctx or id(current_bottle_ctx) != expected_bottle_ctx_signature:
        logger.info(f"AI BottleMatchJob {ai_player_id} in C:{chat_id}: Stale context. Aborting."); return
    if current_bottle_ctx.get('fastest_matcher_id'):
        logger.info(f"AI BottleMatchJob {ai_player_id} in C:{chat_id}: Match claimed by {current_bottle_ctx['fastest_matcher_id']}. Aborting."); return
    
    logger.info(f"AI BottleMatchJob {ai_player_id} in C:{chat_id}: Attempting match with card #{ai_card_idx_to_match}.")
    await handle_bottle_match_attempt(game, context, ai_player_id, ai_card_idx_to_match, is_ai_attempt=True)

async def character_ability_timeout_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data; chat_id = job_data['chat_id']
    expected_context_signature = job_data.get('expected_context_signature')
    
    game = game_state_manager.get_game(chat_id) # Fetch game here for the job
    if not game:
        logger.info(f"Ability timeout for {chat_id}: Game not found. Aborting job.")
        return

    current_active_ability_ctx = game.get('active_ability_context')
    if not current_active_ability_ctx or id(current_active_ability_ctx) != expected_context_signature:
        logger.info(f"Ability timeout for {chat_id}: Stale/completed context. ExpSig {expected_context_signature}, CurId {id(current_active_ability_ctx) if current_active_ability_ctx else 'None'}.")
        return

    user_id = current_active_ability_ctx['player_id']; ability_name = current_active_ability_ctx['card_name']
    player_obj = game_state_manager.get_player_by_id(chat_id, user_id) # Uses chat_id from job_data
    logger.info(f"Ability {ability_name} timed out for P:{user_id} C:{chat_id}. Step: {current_active_ability_ctx.get('step')}")

    if player_obj and not player_obj.get('is_ai'):
        await send_message_to_player(context, user_id, f"Time's up for {ability_name}! Action cancelled.")
    try:
        # Ensure player_obj for mention is fetched using the game object from *this job's context*
        # (already done above)
        mention = get_player_mention(player_obj) if player_obj else "A player"
        await context.bot.send_message(chat_id, f"{mention} ran out of time for {ability_name}.", parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Err sending ability timeout group msg: {e}")

    original_snapshot = None
    if ability_name == "The Killer" and current_active_ability_ctx.get('original_ability_context_snapshot'):
        original_snapshot = current_active_ability_ctx['original_ability_context_snapshot']
    
    game['active_ability_context'] = None # Modify the game object fetched by this job

    if original_snapshot:
        # Pass the game object fetched by this job to resume_original_ability...
        await resume_original_ability_after_killer_interaction(game, context, original_snapshot, "timeout_on_killer_decision")
    else:
        # Pass the game object fetched by this job
        await advance_turn_or_continue_sequence(game, context)

async def advance_turn_or_continue_sequence(game_obj: dict, context: ContextTypes.DEFAULT_TYPE): # CHANGED PARAMETER
    """
    Decides the next step in the game after an action/ability/event concludes.
    It checks for ongoing sequences (like bottle matching, active abilities)
    before advancing to the next player's turn.
    The game_obj is passed directly to avoid re-fetching issues from jobs.
    """
    if not game_obj: # Should ideally not happen if called correctly
        logger.error(f"advance_turn_or_continue_sequence: Received None game_obj. Aborting.")
        # We need a chat_id to inform if possible, but if game_obj is None, we might not have it.
        # This situation indicates a severe problem upstream.
        return

    chat_id = game_obj.get('chat_id') # Get chat_id from the passed game_obj
    if not chat_id:
        logger.error(f"advance_turn_or_continue_sequence: game_obj missing chat_id. Aborting.")
        return

    # Crucial check: Ensure this game object is still the one in the game_state_manager.
    # This protects against the game being ended by another process/thread AFTER
    # the calling function fetched its 'game_obj' but BEFORE this function was entered.
    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game_obj):
        logger.warning(f"advance_turn_or_continue_sequence: Game object for chat {chat_id} has changed or was removed from manager. Passed game_obj might be stale. Aborting.")
        # If game was ended, no further action. If it changed, using stale game_obj is risky.
        return

    # From here, use 'game_obj' as 'game'
    game = game_obj
    logger.info(f"ADV_TURN Entered: C:{chat_id}. Passed game_obj seems valid. Active_games keys: {list(game_state_manager.active_games.keys())}")

    for player in game.get('players', []) + game.get('ai_players', []):
        # We only care about active players running out of cards
        if player.get('status') == PLAYER_STATES["ACTIVE"] and not player.get('hand'):
            logger.info(f"Player {player['id']} has zero cards. Forcing Omerta call.")
            await context.bot.send_message(
                chat_id,
                f"💥 {get_player_mention(player)} has discarded all of their cards! This forces an OMERTA call!",
                parse_mode=ParseMode.HTML
            )
            # The player who ran out of cards is treated as the caller.
            # This is important for scoring, as it makes them eligible for the win.
            await handle_omerta_call(chat_id, context, caller_id=player['id'])
            return

    current_phase = game.get('phase')
    active_ability_ctx_for_log = game.get('active_ability_context') # Get the context, could be None
    active_ability_name_for_log = active_ability_ctx_for_log.get('card_name') if active_ability_ctx_for_log else "None"

    logger.debug(f"advance_turn_or_continue_sequence for C:{chat_id}. Phase: {current_phase}. ActiveAbility: {active_ability_name_for_log}. BottleMatchEndedFlag: {game.get('bottle_match_context_just_ended')}")
    
    # Priority 1: Game is ending or has ended.
    if current_phase in [GAME_PHASES["OMERTA_CALLED"], GAME_PHASES["COMPLETED"]]:
        logger.info(f"Game in chat {chat_id} is ending/completed ({current_phase}). No further turn advancement.")
        game.pop('bottle_match_context_just_ended', None)
        return

    # Priority 2: A bottle match window just finished.
    if game.pop('bottle_match_context_just_ended', False):
        logger.info(f"Advancing turn for chat {chat_id} after bottle match window. Original discarder was: {game.get('current_player_id')}")
        game['phase'] = GAME_PHASES["PLAYING"]
        await start_next_player_turn(game, context) # Pass game object
        return

    # Priority 3: An ability context is still active
    active_ability_ctx = game.get('active_ability_context')
    if active_ability_ctx:
        ability_player = active_ability_ctx.get('player_id')
        ability_name = active_ability_ctx.get('card_name')
        current_step = active_ability_ctx.get('step')
        logger.debug(f"advance_turn_or_continue_sequence for chat {chat_id}: Active ability context exists for P:{ability_player} ({ability_name}, step: {current_step}). Deferring.")
        return

    # Default: No special ongoing sequence, advance to the next player's turn.
    logger.debug(f"advance_turn_or_continue_sequence for chat {chat_id}: No higher priority sequence. Proceeding to start next player turn.")
    if game['phase'] != GAME_PHASES["PLAYING"]:
        game['phase'] = GAME_PHASES["PLAYING"]
    await start_next_player_turn(game, context)

async def process_join_period_end(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    game = game_state_manager.get_game(chat_id)
    if not game or game['phase'] != GAME_PHASES["JOINING"]: logger.info(f"process_join_period_end for {chat_id}: Not in JOINING."); return
    logger.info(f"Join period ended for chat {chat_id}.")
    cancel_job(context, game.get('join_reminder_job_name')); game['join_reminder_job_name'] = None
    num_total_players = len(game.get('players', [])) + len(game.get('ai_players', []))
    if game.get('join_message_id'): 
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=game['join_message_id'], text=f"Joining ended. Players: {num_total_players}.", reply_markup=None)
            game['join_message_id'] = None 
        except TelegramError: pass 
    if num_total_players < MIN_PLAYERS:
        try: await context.bot.send_message(chat_id=chat_id, text=f"Not enough players ({num_total_players}). Need {MIN_PLAYERS}. Game cancelled. /newgame to try again.")
        except TelegramError as e: logger.error(f"Err sending 'not enough players': {e}")
        game_state_manager.end_game(chat_id); return
    await initiate_game_start_sequence(chat_id, context)

async def check_all_players_viewed_cards(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    game = game_state_manager.get_game(chat_id)
    if not game or game['phase'] != GAME_PHASES["VIEWING"]: return
    all_human_players_viewed = all(p.get('viewed_all_initial_cards', False) for p in game.get('players',[]) if not p.get('is_ai'))
    if all_human_players_viewed:
        logger.info(f"All humans in {chat_id} viewed cards early.")
        job_name_to_cancel = game.get('viewing_timer_job_name')
        if cancel_job(context, job_name_to_cancel): logger.info(f"Cancelled viewing timer {job_name_to_cancel} for {chat_id}.")
        game['viewing_timer_job_name'] = None 
        class DummyJobContext: 
            def __init__(self, c_id, j_name): self.data = {'chat_id': c_id, 'expected_viewing_timer_job_name': j_name} # Match expected data
        dummy_ptb_ctx = ContextTypes.DEFAULT_TYPE(application=context.application, chat_id=chat_id, user_id=None)
        # Pass the job name that *was* scheduled, so viewing_timeout_job can compare if it was stale (though it's being called directly)
        dummy_ptb_ctx.job = DummyJobContext(chat_id, job_name_to_cancel) 
        await viewing_timeout_job(dummy_ptb_ctx)

async def handle_omerta_call(chat_id: int, context: ContextTypes.DEFAULT_TYPE, 
                             caller_id: Optional[Union[int, str]], 
                             forced_by_no_players: bool = False, 
                             forced_by_empty_deck: bool = False):
    """Processes an Omerta call, calculates scores, and ends the round."""
    game = game_state_manager.get_game(chat_id)
    if not game: 
        logger.error(f"handle_omerta_call: No game found for chat {chat_id}.")
        return
    
    # Prevent re-entry if Omerta already called or game completed
    if game['phase'] in [GAME_PHASES["OMERTA_CALLED"], GAME_PHASES["COMPLETED"]]:
        logger.info(f"handle_omerta_call for chat {chat_id} ignored, already in phase {game['phase']}.")
        return

    # Cancel any ongoing ability timeouts or bottle matching if Omerta is called
    active_ctx = game.get('active_ability_context')
    if active_ctx and active_ctx.get('timeout_job_name'):
        cancel_job(context, active_ctx['timeout_job_name'])
        game['active_ability_context'] = None # Clear context as Omerta overrides it
    
    bottle_ctx = game.get('bottle_match_context')
    if bottle_ctx and bottle_ctx.get('timeout_job_name'):
        cancel_job(context, bottle_ctx['timeout_job_name'])
        game['bottle_match_context'] = None

    game['phase'] = GAME_PHASES["OMERTA_CALLED"]
    game['omerta_caller_id'] = caller_id
    caller_player_obj = game_state_manager.get_player_by_id(chat_id, caller_id) if caller_id else None
    
    omerta_announcement = "🚨 OMERTA CALLED! 🚨\nRevealing all hands and calculating scores..."
    if caller_player_obj:
        omerta_announcement = f"🚨 OMERTA CALLED by {get_player_mention(caller_player_obj)}! 🚨\nRevealing all hands..."
    elif forced_by_no_players:
        omerta_announcement = "🚨 No active players left! Game ending, scores will be calculated.🚨"
    elif forced_by_empty_deck:
        omerta_announcement = "🚨 Deck is depleted! Game ending, scores will be calculated.🚨"
    
    try: await context.bot.send_message(chat_id, omerta_announcement, parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Error sending Omerta announcement: {e}")

    results_text_parts = ["<b>Final Scores & Hands:</b>"]
    
    # Use active players at the point Omerta was called
    # Fetch all participants who might have a score (even if they became inactive this turn)
    all_participants_at_end = game.get('players', []) + game.get('ai_players', [])
    # However, scoring should ideally only count for those who were 'ACTIVE' just before Omerta.
    # For simplicity now, we'll score everyone listed in players/ai_players in the game state.
    # A more refined approach might use game['turn_order'] if it always reflects active players.

    active_players_for_scoring = [p for p in all_participants_at_end if p.get('status') == PLAYER_STATES["ACTIVE"] or p.get('id') == caller_id]
    if not active_players_for_scoring and caller_id: # If only caller is left, add them.
        if caller_player_obj and caller_player_obj not in active_players_for_scoring:
             active_players_for_scoring.append(caller_player_obj)
    if not active_players_for_scoring: # Still no one? This is odd.
        logger.warning(f"Omerta call in chat {chat_id}, but no active players found for scoring.")
        active_players_for_scoring = all_participants_at_end # Fallback to all

    for p_data in all_participants_at_end: # Iterate all to show hands, but score based on active
        hand_value = game_state_manager.calculate_score_for_hand(p_data.get('hand', []))
        p_data['score_this_round'] = hand_value 
        
        hand_str_parts = []
        for card in p_data.get('hand', []):
            card_display = escape_html(card.get('name', 'Unknown Card'))
            card_points = card.get('points', card.get('value', '?'))
            hand_str_parts.append(f"{card_display} ({card_points} pts)")
        hand_str = ", ".join(hand_str_parts) if hand_str_parts else "Empty Hand"
        
        score_display = f"<b>{hand_value} pts</b>"
        if p_data.get('status') != PLAYER_STATES["ACTIVE"] and p_data.get('id') != caller_id:
            score_display += " (Inactive)"
            p_data['score_this_round'] = 999 # Penalize inactive players heavily for sorting unless they are the caller

        results_text_parts.append(f"{get_player_mention(p_data)}: {score_display} (Hand: {hand_str})")

    # Determine winner and apply Omerta penalty
    winner_obj = None
    # Filter again for truly active players for win condition logic, caller is always considered.
    eligible_for_win = [p for p in active_players_for_scoring if p.get('status') == PLAYER_STATES["ACTIVE"] or p.get('id') == caller_id]


    if eligible_for_win:
        # Sort by score (lowest first) amongst eligible players
        sorted_eligible_players = sorted(eligible_for_win, key=lambda p: p.get('score_this_round', 999))
        actual_lowest_scorer = sorted_eligible_players[0] if sorted_eligible_players else None

        if caller_player_obj and caller_player_obj in eligible_for_win: # Omerta called by an eligible player
            caller_score = caller_player_obj.get('score_this_round', 999)
            if actual_lowest_scorer and caller_player_obj['id'] == actual_lowest_scorer['id'] and caller_score <= OMERTA_THRESHOLD:
                results_text_parts.append(f"\n🎉 {get_player_mention(caller_player_obj)} called Omerta perfectly (Score {caller_score} ≤ {OMERTA_THRESHOLD} and lowest) and <b>WINS THE ROUND!</b> 🎉")
                winner_obj = caller_player_obj
            else: # Incorrect Omerta call
                penalty_msg = f"\n{get_player_mention(caller_player_obj)}'s Omerta call was not successful."
                if not actual_lowest_scorer or caller_player_obj['id'] != actual_lowest_scorer['id']:
                    penalty_msg += f" Score ({caller_score}) was not the lowest."
                elif caller_score > OMERTA_THRESHOLD:
                     penalty_msg += f" Score ({caller_score}) was > {OMERTA_THRESHOLD} threshold."
                
                caller_player_obj['score_this_round'] = caller_score + OMERTA_PENALTY
                penalty_msg += f" A {OMERTA_PENALTY} point penalty is applied. New score: <b>{caller_player_obj['score_this_round']}</b>."
                results_text_parts.append(penalty_msg)
                
                # Re-sort eligible players with penalty to find the true winner
                sorted_eligible_players_after_penalty = sorted(eligible_for_win, key=lambda p: p.get('score_this_round', 999))
                winner_obj = sorted_eligible_players_after_penalty[0] if sorted_eligible_players_after_penalty else None
                if winner_obj:
                    results_text_parts.append(f"\n🏆 The actual winner of the round is {get_player_mention(winner_obj)} with <b>{winner_obj.get('score_this_round')} points!</b> 🏆")
        elif actual_lowest_scorer : # Omerta forced by game system (no caller penalty)
            winner_obj = actual_lowest_scorer
            results_text_parts.append(f"\n🏆 With the game ending, {get_player_mention(winner_obj)} has the lowest score of <b>{winner_obj.get('score_this_round')} points</b> and wins! 🏆")
    
    if not winner_obj and eligible_for_win: # Should find a winner if there are eligible players
        logger.warning(f"No winner determined in Omerta for chat {chat_id} despite eligible players.")
        results_text_parts.append("\nNo winner could be determined due to scoring anomaly.")
    elif not eligible_for_win:
         results_text_parts.append("\nNo players were eligible for winning this round.")


    # Determine ultimate loser (highest score among those who had a score calculated for this round)
    scored_players_for_loser = [p for p in all_participants_at_end if 'score_this_round' in p and p.get('status') != PLAYER_STATES["INACTIVE"]] # Exclude those marked inactive before omerta for loser calc
    if not scored_players_for_loser and caller_player_obj : scored_players_for_loser.append(caller_player_obj) # ensure caller is considered if no one else

    if scored_players_for_loser:
        # Sort by score_this_round, highest first.
        # Handle potential None scores if 'score_this_round' wasn't set for some edge case.
        ultimate_loser = max(scored_players_for_loser, key=lambda p: p.get('score_this_round', -1)) 
        if 'score_this_round' in ultimate_loser and ultimate_loser.get('score_this_round', -1) > -1 : 
             results_text_parts.append(f"\n💀 The 'Ultimate Loser' (highest score) is {get_player_mention(ultimate_loser)} with <b>{ultimate_loser.get('score_this_round')} points</b>.")
    
    try: await context.bot.send_message(chat_id, "\n".join(results_text_parts), parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Error sending Omerta results: {e}")
    
    final_scores_list = []
    winner_id_for_db = winner_obj['id'] if winner_obj else None
    
    # We use all_participants_at_end to ensure everyone who played gets stats recorded
    for p_data in all_participants_at_end:
        final_scores_list.append({
            'id': p_data['id'],
            'name': p_data.get('first_name', 'Player'),
            'score': p_data.get('score_this_round', 999), # Use the final calculated score
            'is_winner': p_data.get('id') == winner_id_for_db,
            'is_ai': p_data.get('is_ai', False)
        })

    # Add the list to the game object for the database handler
    game['final_scores_list'] = final_scores_list
    db.update_player_stats(game)
    logger.info(f"Player stats for game in chat {chat_id} have been updated in the database.")

    
    game['phase'] = GAME_PHASES["COMPLETED"]
    try:
        await context.bot.send_message(chat_id, "The round has concluded. Wanna play another hand, boss?", 
                                   reply_markup=keyboards.get_play_again_keyboard(), parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Error sending play again message: {e}")

async def decrement_blocked_cards_at_cycle_start(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    game = game_state_manager.get_game(chat_id)
    if not game or not game.get('blocked_cards'): 
        logger.debug(f"DecrementBlockedCards: No game or no blocked_cards for chat {chat_id}.")
        return

    logger.info(f"DecrementBlockedCards: Checking and decrementing Police Patrol blocks for chat {chat_id} at start of cycle {game.get('cycle_count')}.")
    updated_any_blocks = False
    
    # Iterate over a copy of player_ids if we might delete from game['blocked_cards']
    for player_id_str_or_int, blocked_map in list(game.get('blocked_cards', {}).items()):
        player_id = int(player_id_str_or_int) if isinstance(player_id_str_or_int, str) and player_id_str_or_int.isdigit() else player_id_str_or_int
        
        # Iterate over a copy of card_indices if we might delete from blocked_map
        for card_idx, cycles_left in list(blocked_map.items()):
            blocked_map[card_idx] = cycles_left - 1
            logger.debug(f"DecrementBlockedCards: Player {player_id}, Card #{card_idx+1} now has {blocked_map[card_idx]} cycles left.")
            if blocked_map[card_idx] <= 0:
                del blocked_map[card_idx]
                updated_any_blocks = True
                target_player = game_state_manager.get_player_by_id(chat_id, player_id)
                if target_player:
                    card_name_unblocked = "Unknown" 
                    try:
                        if 0 <= card_idx < len(target_player.get('hand',[])):
                            card_name_unblocked = target_player['hand'][card_idx]['name']
                    except Exception: pass 
                    unblock_msg_pm = f"The Police Patrol on your Card at Position #{card_idx+1} (currently {escape_html(card_name_unblocked)}) has moved on. It's no longer blocked."
                    unblock_msg_group = f"The block on {get_player_mention(target_player)}'s Card at Position #{card_idx+1} has expired."
                    if not target_player.get('is_ai'):
                        await send_message_to_player(context, target_player['id'], unblock_msg_pm, parse_mode=ParseMode.HTML)
                    try: await context.bot.send_message(chat_id, unblock_msg_group, parse_mode=ParseMode.HTML)
                    except TelegramError as e: logger.error(f"Error sending unblock notification: {e}")

        if not blocked_map: # No more blocked cards for this player
            if player_id_str_or_int in game['blocked_cards']: # Check before del
                del game['blocked_cards'][player_id_str_or_int]
    
    if updated_any_blocks:
        logger.info(f"DecrementBlockedCards: Police Patrol block cycles updated for chat {chat_id}.")

async def handle_ai_player_turn(game_obj: dict, context: ContextTypes.DEFAULT_TYPE, ai_player_id: str):
    game = game_obj
    ai_player = game_state_manager.get_player_by_id(game['chat_id'], ai_player_id)
    if not game or not ai_player or not ai_player.get('is_ai'): 
        logger.error(f"handle_ai_player_turn: Invalid call for AI {ai_player_id} in chat {game['chat_id']}. Game or AI player not found.")
        # If AI turn cannot proceed, try to advance to next player to prevent game stall
        if game: await advance_turn_or_continue_sequence(game, context)
        return

    logger.info(f"handle_ai_player_turn: ENTERED for AI Player {ai_player_id} in chat {game['chat_id']}.")
    try:
        await context.bot.send_message(game['chat_id'], f"{get_player_mention(ai_player)} is pondering their next move...", parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Error sending AI thinking message: {e}")
    
    await asyncio.sleep(random.uniform(1.5, 3.0)) # Simulate AI "thinking"

    # --- AI Omerta Call Logic (Placeholder - AI currently doesn't call Omerta) ---
    # if ai_should_call_omerta_heuristic(game, ai_player):
    #    logger.info(f"AI {ai_player_id} decided to call Omerta.")
    #    await handle_omerta_call(chat_id, context, ai_player_id)
    #    return 

    # --- AI Draw Decision (Deck vs Discard - Placeholder) ---
    # For now, AI always tries to draw from deck if available.
    # Future: AI could check discard pile for valid cards (Bottle, Alibi).
    action_taken_msg_part = "drew from the deck"
    drawn_card = None
    replaced_card = None

    if not ai_player.get('hand'): # AI has no cards, should not happen if game is running correctly
        logger.error(f"AI {ai_player_id} has no hand to perform a turn. This is unexpected. Forcing Omerta.")
        await handle_omerta_call(game['chat_id'], context, ai_player_id, forced_by_empty_deck=True) # Treat as game stuck
        return

    if not game['deck']: # Deck is empty
        logger.info(f"AI {ai_player_id}: Deck is empty. Attempting to reshuffle discard pile.")
        if game['discard_pile']:
            top_discard_card_obj = game['discard_pile'].pop() if len(game['discard_pile']) > 0 else None
            game['deck'] = game['discard_pile'][:] # Copy remaining to deck
            random.shuffle(game['deck'])
            game['discard_pile'] = [top_discard_card_obj] if top_discard_card_obj else []
            try: 
                reshuffle_msg = "Deck was empty. AI observes as the discard pile is reshuffled."
                if not game['deck'] : reshuffle_msg += " Still no cards to draw!"
                await context.bot.send_message(game['chat_id'], reshuffle_msg, parse_mode=ParseMode.HTML)
            except TelegramError as e: logger.error(f"Error sending reshuffle message: {e}")
        
        if not game['deck']: # Still empty after reshuffle attempt
            logger.warning(f"AI {ai_player_id} finds no cards to draw even after reshuffle attempt! Forcing Omerta.")
            await handle_omerta_call(game['chat_id'], context, ai_player_id, forced_by_empty_deck=True)
            return
            
    drawn_card = game['deck'].pop() 
    
    # AI replaces a random card from its hand
    if not ai_player['hand']: # Should have been caught earlier, but double check
        logger.error(f"AI {ai_player_id} had no hand to replace card after drawing. Discarding drawn card {drawn_card.get('name')}.")
        game['discard_pile'].append(drawn_card)
        replaced_card = drawn_card # Conceptually, the drawn card is immediately discarded
    else:
        card_to_replace_idx = random.randrange(len(ai_player['hand']))
        replaced_card = ai_player['hand'][card_to_replace_idx]
        ai_player['hand'][card_to_replace_idx] = drawn_card
        game['discard_pile'].append(replaced_card) # Add the AI's replaced card to discard

    discard_msg_text = (f"{get_player_mention(ai_player)} {action_taken_msg_part}, "
                        f"replaced Card #{card_to_replace_idx+1 if replaced_card != drawn_card else 'drawn card directly'}, "
                        f"and discarded <b>{escape_html(replaced_card.get('name'))}</b> "
                        f"({replaced_card.get('points', replaced_card.get('value', '?'))} pts).")
    try:
        await context.bot.send_message(game['chat_id'], discard_msg_text, parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Error sending AI discard message: {e}")

    logger.info(f"AI {ai_player_id} completed main action, discarded {replaced_card.get('name')}. Processing this discard.")
    # The AI's "turn action" (draw/replace) is done. Now process the card it put on the discard pile.
    # is_ai_discard=True in process_discarded_card will now be set based on player_obj_who_discarded.get('is_ai')
    await process_discarded_card(game, context, ai_player_id, replaced_card)
    # `process_discarded_card` will handle:
    #   - Initiating bottle match window (if bottle)
    #   - Initiating character ability (if character), which then has AI-specific execution paths in initiate_character_ability.
    #   - If neither of above, it calls `advance_turn_or_continue_sequence`.

async def _start_turn_for_current_player(game: dict, context: ContextTypes.DEFAULT_TYPE):
    """
    Initiates the turn for the player currently set in game['current_player_id'].
    This is used for starting the very first turn of the game after viewing.
    """
    chat_id = game['chat_id']
    player_id = game.get('current_player_id')
    
    if not player_id:
        logger.error(f"_start_turn_for_current_player: No current_player_id in game {chat_id}. Ending game.")
        await context.bot.send_message(chat_id, "Critical error: No starting player found. Game ended.")
        game_state_manager.end_game(chat_id)
        return

    player_obj = game_state_manager.get_player_by_id(chat_id, player_id)
    if not player_obj:
        logger.error(f"_start_turn_for_current_player: Player object for {player_id} not found. Advancing.")
        await start_next_player_turn(game, context) # Fallback to advancing turn to unstick game
        return

    # Announce turn
    turn_announce_text = f"🚬 <b>Cycle {game['cycle_count']}</b> - Turn for {get_player_mention(player_obj)}."
    logger.info(f"_start_turn_for_current_player: Announcing turn for {player_id}. Cycle: {game['cycle_count']}")
    try:
        await context.bot.send_message(chat_id, turn_announce_text, parse_mode=ParseMode.HTML)
    except TelegramError as e:
        logger.error(f"_start_turn_for_current_player: Error sending turn announcement: {e}")

    # Handle turn based on player type
    if player_obj.get('is_ai'):
        await handle_ai_player_turn(game, context, player_obj['id'])
    else: # Human player
        logger.info(f"_start_turn_for_current_player: Human player {player_obj['id']}'s turn. Sending action PM.")
        is_al_capone_first_ever_turn = (game['cycle_count'] == 1 and player_obj['id'] == game.get('al_capone_player_id'))
        if is_al_capone_first_ever_turn:
            game['al_capone_first_turn_taken'] = True

        action_keyboard = keyboards.get_player_turn_actions_keyboard(game, player_obj, is_al_capone_first_ever_turn)
        pm_player_id = int(player_obj['id']) if isinstance(player_obj['id'], str) and str(player_obj['id']).isdigit() else player_obj['id']
        await send_message_to_player(context, pm_player_id, "It's your turn! Choose an action:", reply_markup=action_keyboard)

async def start_next_player_turn(game_obj: dict, context: ContextTypes.DEFAULT_TYPE):
    game = game_obj 
    chat_id = game.get('chat_id')

    if not chat_id:
        logger.error(f"start_next_player_turn: chat_id is missing from game_obj. Aborting. Game: {game}")
        # Depending on how critical this is, you might want to end the game or raise an error
        return

    logger.info(f"start_next_player_turn: ENTERED for chat {chat_id} with passed game_obj.")

    if game['phase'] != GAME_PHASES["PLAYING"]:
        logger.warning(f"start_next_player_turn called for chat {chat_id} in wrong phase: {game.get('phase')}. Expected PLAYING. Correcting.")
        game['phase'] = GAME_PHASES["PLAYING"]

    if not game.get('turn_order'):
        logger.error(f"start_next_player_turn: game['turn_order'] is empty for chat {chat_id}. Forcing Omerta.")
        await context.bot.send_message(chat_id, "Error: Turn order is missing. The game will now end.", parse_mode=ParseMode.HTML)
        await handle_omerta_call(chat_id, context, None, forced_by_no_players=True)
        return

    current_player_id_for_finding_next = game.get('current_player_id') # Player whose turn just ended (or None for first turn)
    original_turn_order_list = game['turn_order'] # Use the game's canonical turn order
    num_total_in_order = len(original_turn_order_list)

    if num_total_in_order == 0:
        logger.error(f"start_next_player_turn: No players in original_turn_order_list for chat {chat_id}. Forcing Omerta.")
        await handle_omerta_call(chat_id, context, None, forced_by_no_players=True)
        return

    start_index = 0
    if current_player_id_for_finding_next is not None:
        try:
            current_idx_in_original_list = next(i for i, p in enumerate(original_turn_order_list) if p['id'] == current_player_id_for_finding_next)
            start_index = (current_idx_in_original_list + 1) % num_total_in_order
        except StopIteration:
            logger.warning(f"start_next_player_turn: Previous player {current_player_id_for_finding_next} not in original turn order. Defaulting to first player in order.")
            start_index = 0 # Fallback
    
    next_player_obj_candidate = None
    
    # Loop through the turn order to find the next eligible player
    for i in range(num_total_in_order):
        prospective_player_index = (start_index + i) % num_total_in_order
        player_to_check = original_turn_order_list[prospective_player_index]

        if player_to_check.get('status') == PLAYER_STATES["INACTIVE"]:
            continue # Skip inactive players

        # Found a non-inactive player. This is our candidate.
        next_player_obj_candidate = player_to_check
        break 
    
    if not next_player_obj_candidate:
        logger.error(f"start_next_player_turn: No eligible (non-inactive) players found in turn order for chat {chat_id}. Forcing Omerta.")
        await context.bot.send_message(chat_id, "No active players left to take a turn. The game will now end.", parse_mode=ParseMode.HTML)
        await handle_omerta_call(chat_id, context, None, forced_by_no_players=True)
        return

    # Now, process the found candidate
    game['current_player_id'] = next_player_obj_candidate['id'] # Set current player
    next_player_obj = next_player_obj_candidate # For clarity in subsequent code

    # Cycle count update logic (needs to use current_player_id_for_finding_next for comparison)
    is_start_of_new_cycle = False
    original_al_capone_id = game.get('al_capone_player_id')
    # Check if the *newly selected current player* is Al Capone and if it's not the game's very first turn by AC
    if next_player_obj['id'] == original_al_capone_id:
        if game['cycle_count'] > 0 or (current_player_id_for_finding_next is not None and next_player_obj['id'] != original_turn_order_list[0]['id']):
            if game.get('current_player_id_just_before_cycle_check') != next_player_obj['id'] or game['cycle_count'] > 0:
                 game['cycle_count'] += 1
                 is_start_of_new_cycle = True
                 logger.info(f"start_next_player_turn: New cycle {game['cycle_count']} starting with Al Capone ({original_al_capone_id}). Decrementing blocks.")
                 await decrement_blocked_cards_at_cycle_start(chat_id, context)

                 if game['cycle_count'] > 2:
                     logger.info(f"Cycle {game['cycle_count']} > 2. Announcing player hand counts for chat {chat_id}.")
                     hand_count_parts = [f"<b>Hand-O-Meter (Cycle {game['cycle_count']}):</b>"]
                     
                     # We get all active players to show their counts
                     all_active_players = [p for p in game.get('players', []) + game.get('ai_players', []) if p.get('status') in [PLAYER_STATES["ACTIVE"], PLAYER_STATES["SKIPPED_TURN"]]]
                     
                     for p_data in all_active_players:
                         player_mention = get_player_mention(p_data)
                         hand_size = len(p_data.get('hand', []))
                         hand_count_parts.append(f"- {player_mention}: {hand_size} card(s)")
                         
                     if len(hand_count_parts) > 1:
                         try:
                             await context.bot.send_message(chat_id, "\n".join(hand_count_parts), parse_mode=ParseMode.HTML)
                         except TelegramError as e:
                             logger.error(f"Failed to send hand count summary for chat {chat_id}: {e}")
                 # --- END OF NEW LOGIC ---

    if game['cycle_count'] == 0: # If still 0, it's the first cycle of the game
        game['cycle_count'] = 1
        logger.info(f"start_next_player_turn: First turn of game, cycle set to {game['cycle_count']}.")
    game['current_player_id_just_before_cycle_check'] = next_player_obj['id'] # Helper for next cycle check

    # Handle SKIPPED_TURN status for the chosen next_player_obj
    if next_player_obj.get('status') == PLAYER_STATES["SKIPPED_TURN"]:
        logger.info(f"start_next_player_turn: Player {next_player_obj['id']}'s turn is skipped.")
        await context.bot.send_message(chat_id, f"{get_player_mention(next_player_obj)}'s turn is skipped (Mamma's orders!).", parse_mode=ParseMode.HTML)
        
        next_player_obj['status'] = PLAYER_STATES["ACTIVE"] # Player becomes active for the turn *after* the skipped one.
        if 'cannot_call_omerta' in next_player_obj: 
            next_player_obj['cannot_call_omerta'] = False # Cleared after skip
        
        logger.debug(f"start_next_player_turn: Scheduling next turn advance due to skip for {next_player_obj['id']}. Current player was already updated to them.")
        # The current_player_id is already this skipped player. The job will advance from them.
        context.job_queue.run_once(lambda ctx: asyncio.create_task(start_next_player_turn(game, ctx)), 0.2, name=f"skip_adv_{chat_id}_{next_player_obj['id']}")
        return # IMPORTANT: Return here

    # If not skipped, proceed with the turn announcement and action
    turn_announce_text = f"🚬 <b>Cycle {game['cycle_count']}</b> - Turn for {get_player_mention(next_player_obj)}."
    logger.info(f"start_next_player_turn: Announcing turn for {next_player_obj['id']}. Cycle: {game['cycle_count']}")
    try:
        await context.bot.send_message(chat_id, turn_announce_text, parse_mode=ParseMode.HTML)
    except TelegramError as e:
        logger.error(f"start_next_player_turn: Error sending turn announcement: {e}")

    if next_player_obj.get('is_ai'):
        await handle_ai_player_turn(game, context, next_player_obj['id'])
    else: # Human player
        logger.info(f"start_next_player_turn: Human player {next_player_obj['id']}'s turn. Sending action PM.")
        # Determine if this is Al Capone's very first turn in the game (Cycle 1 and AC's ID)
        is_al_capone_first_ever_turn = (game['cycle_count'] == 1 and next_player_obj['id'] == game.get('al_capone_player_id') and not game.get('al_capone_first_turn_taken', False))
        if is_al_capone_first_ever_turn:
            game['al_capone_first_turn_taken'] = True # Mark that AC's first turn processing has occurred

        action_keyboard = keyboards.get_player_turn_actions_keyboard(game, next_player_obj, is_al_capone_first_ever_turn)
        pm_player_id = int(next_player_obj['id']) if isinstance(next_player_obj['id'], str) and next_player_obj['id'].isdigit() else next_player_obj['id']
        await send_message_to_player(context, pm_player_id, "It's your turn! Choose an action:", reply_markup=action_keyboard)
    
async def handle_bottle_match_attempt(game_obj: dict, context: ContextTypes.DEFAULT_TYPE, # CHANGED game_obj
                                      player_id: Union[int,str],
                                      card_idx_to_discard: int,
                                      is_ai_attempt: bool = False):
    game = game_obj

    if not game:
        logger.error(f"HBM_Attempt: game_obj is None. Player: {player_id}, CardIdx: {card_idx_to_discard}.")
        return

    chat_id = game.get('chat_id')
    if not chat_id:
        logger.error(f"HBM_Attempt: chat_id missing from game_obj for P:{player_id}. Aborting.")
        return

    # Stale game check
    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"HBM_Attempt: Stale game_obj for C:{chat_id} (P:{player_id}). Aborting.")
        return

    player_data = game_state_manager.get_player_by_id(chat_id, player_id)
    bottle_context = game.get('bottle_match_context')

    logger.info(f"HBM_Attempt: Entered for P:{player_id} (AI:{is_ai_attempt}) C_Idx:{card_idx_to_discard} in C:{chat_id}. Game Phase: {game.get('phase')}. BottleCtx: {'Exists' if bottle_context else 'None'}")

    # --- VALIDATION CHECKS ---
    if not player_data or not bottle_context or game.get('phase') != GAME_PHASES["BOTTLE_MATCHING_WINDOW"]:
        logger.info(f"HBM_Attempt by P:{player_id}: Window closed, phase mismatch, context missing, or player missing.")
        if not is_ai_attempt and hasattr(context, 'callback_query') and context.callback_query and context.callback_query.id:
            try: await query.answer("Too late or invalid action for bottle matching.", show_alert=True)
            except TelegramError: pass
        return

    if bottle_context.get('fastest_matcher_id'):
        logger.info(f"HBM_Attempt by P:{player_id}: Match already claimed by {bottle_context['fastest_matcher_id']}.")
        if not is_ai_attempt and hasattr(context, 'callback_query') and context.callback_query and context.callback_query.id:
            try: await context.callback_query.answer("Too slow! Someone else already matched.", show_alert=True)
            except TelegramError: pass
        return

    if player_id in bottle_context.get('failed_matchers', set()):
        logger.info(f"HBM_Attempt by P:{player_id}: Blocked, player already failed a match in this window.")
        if not is_ai_attempt and hasattr(context, 'callback_query') and context.callback_query and context.callback_query.id:
            try: await context.callback_query.answer("You already tried and failed to match. You cannot try again this round.", show_alert=True)
            except TelegramError: pass
        return

    if not (0 <= card_idx_to_discard < len(player_data.get('hand',[]))):
        logger.warning(f"HBM_Attempt by P:{player_id}: Invalid card index {card_idx_to_discard}.")
        if not is_ai_attempt and hasattr(context, 'callback_query') and context.callback_query and context.callback_query.id:
            try: await context.callback_query.answer("Invalid card selected.", show_alert=True)
            except TelegramError: pass
        return

    card_to_match_with = player_data['hand'][card_idx_to_discard]
    logger.debug(f"HBM_Attempt: P:{player_id} attempting with {card_to_match_with.get('name')} (val:{card_to_match_with.get('value')}). Target: {bottle_context.get('discarded_card_value')}")

    # --- LOGIC FOR SUCCESSFUL MATCH ---
    if card_to_match_with.get('type') == 'bottle' and card_to_match_with.get('value') == bottle_context.get('discarded_card_value'):
        bottle_context['fastest_matcher_id'] = player_id
        
        logger.debug(f"HBM_Attempt: Matched by P:{player_id}. Cancelling timeout job: {bottle_context.get('timeout_job_name')}")
        cancel_job(context, bottle_context.get('timeout_job_name'))

        matched_card_object = player_data['hand'].pop(card_idx_to_discard)
        game.setdefault('discard_pile', []).append(matched_card_object)
        logger.info(f"HBM_Attempt by P:{player_id}: Successfully matched bottle.")

        success_pm_text = f"Success! You matched and discarded your {escape_html(matched_card_object.get('name'))}."
        success_group_text = (f"⚡️ Quick draw! {get_player_mention(player_data)} "
                              f"matched Bottle {bottle_context.get('discarded_card_value')} and discarded their <b>{escape_html(matched_card_object.get('name'))}</b>!")
        
        if not is_ai_attempt:
            if hasattr(context, 'callback_query') and context.callback_query and context.callback_query.id:
                try: await context.callback_query.answer("Match successful!", show_alert=False)
                except TelegramError: pass
            await send_message_to_player(context, player_id, success_pm_text)

        try: await context.bot.send_message(chat_id, success_group_text, parse_mode=ParseMode.HTML)
        except TelegramError as e: logger.error(f"HBM_Attempt: Error sending group success msg: {e}")
        
        for p_id_notified, msg_id in bottle_context.get('notified_players_pm_ids', {}).items():
            if p_id_notified != player_id:
                try:
                    chat_id_for_edit = int(p_id_notified) if str(p_id_notified).isdigit() else p_id_notified
                    await context.bot.edit_message_text(chat_id=chat_id_for_edit, message_id=msg_id, text="Bottle successfully matched by another player!", reply_markup=None)
                except TelegramError: pass
        
        triggering_player_id = bottle_context.get('triggering_player_id')
        game['bottle_match_context'] = None
        game['bottle_match_context_just_ended'] = True
        game['current_player_id'] = triggering_player_id
        logger.info(f"HBM_Attempt Success: Current player set to {triggering_player_id}. Calling advance_turn.")
        await advance_turn_or_continue_sequence(game, context)
        return

    # --- LOGIC FOR FAILED MATCH (with updated messages) ---
    else:
        logger.info(f"HBM_Attempt by P:{player_id}: FAILED match. Card {card_to_match_with.get('name')} is NOT a match for Bottle {bottle_context.get('discarded_card_value')}.")
        
        bottle_context.setdefault('failed_matchers', set()).add(player_id)

        failed_card_name = escape_html(card_to_match_with.get('name', 'the chosen card'))
        penalty_msg_player = f"Oops! '{failed_card_name}' is not the right bottle. You can't try again this round."
        penalty_msg_group = f"⚠️ {get_player_mention(player_data)} tried to match with <b>{failed_card_name}</b> but failed!"
        
        if game.get('deck'):
            penalty_card = game['deck'].pop()
            player_data.setdefault('hand', []).append(penalty_card)
            logger.info(f"HBM_Attempt Fail: Player {player_id} drew penalty card '{penalty_card.get('name')}' (hidden from players).")
            # Add text about the penalty card now that we know one was drawn
            penalty_msg_player += "\nYou have received a penalty card from the deck."
            penalty_msg_group += " They receive a penalty card."
        else:
            logger.warning(f"HBM_Attempt Fail: Deck empty, cannot give penalty card to player {player_id}.")
            penalty_msg_player += "\n(The deck was empty, so you got lucky... no card drawn.)"
            penalty_msg_group += " (The deck was empty, so no card drawn.)"

        if not is_ai_attempt:
            if hasattr(context, 'callback_query') and context.callback_query and context.callback_query.id:
                try: await context.callback_query.answer(f"Wrong card! You get a penalty.", show_alert=True)
                except TelegramError: pass
            pm_message_id_to_edit = bottle_context.get('notified_players_pm_ids', {}).get(player_id)
            if pm_message_id_to_edit:
                try:
                    await context.bot.edit_message_text(chat_id=player_id, message_id=pm_message_id_to_edit, text=penalty_msg_player, reply_markup=None)
                except TelegramError as e_edit:
                    logger.warning(f"HBM_Attempt Fail: Could not edit PM for player {player_id}: {e_edit}")
            else:
                await send_message_to_player(context, player_id, penalty_msg_player)
        
        try:
            await context.bot.send_message(chat_id, penalty_msg_group, parse_mode=ParseMode.HTML)
        except TelegramError as e_group:
            logger.error(f"HBM_Attempt Fail: Error sending group penalty message: {e_group}")
        
        return
    
async def end_bottle_match_window_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    chat_id = job_data['chat_id']
    expected_bottle_ctx_signature = job_data.get('expected_bottle_ctx_signature')

    game = game_state_manager.get_game(chat_id) # Fetch game here for the job
    if not game:
        logger.info(f"EndBottleMatchJob: No game for {chat_id} when job started. Aborting.")
        return

    logger.debug(f"EndBottleMatchJob: Fetched game for {chat_id}. Game phase: {game.get('phase')}, Bottle Ctx ID: {id(game.get('bottle_match_context')) if game.get('bottle_match_context') else 'None'}")
    current_bottle_ctx = game.get('bottle_match_context')

    if game.get('phase') != GAME_PHASES["BOTTLE_MATCHING_WINDOW"] or \
       not current_bottle_ctx or \
       (expected_bottle_ctx_signature and id(current_bottle_ctx) != expected_bottle_ctx_signature):
        logger.info(f"EndBottleMatchJob for {chat_id}: Window already closed, phase mismatch, or stale context. ExpSig: {expected_bottle_ctx_signature}, CurCtxId: {id(current_bottle_ctx) if current_bottle_ctx else 'None'}. Game Phase: {game.get('phase')}")
        return

    logger.info(f"Bottle match window TIMEOUT for chat {chat_id}. Fastest matcher in context: {current_bottle_ctx.get('fastest_matcher_id')}")

    for p_id, msg_id in current_bottle_ctx.get('notified_players_pm_ids', {}).items():
        if not current_bottle_ctx.get('fastest_matcher_id') or p_id != current_bottle_ctx.get('fastest_matcher_id'):
            try:
                await context.bot.edit_message_text(chat_id=p_id, message_id=msg_id, text="Time's up for bottle matching!", reply_markup=None)
            except TelegramError:
                pass

    if not current_bottle_ctx.get('fastest_matcher_id'):
        try:
            await context.bot.send_message(chat_id, "Bottle matching window closed. No one snatched the bottle in time.", parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.error(f"Error sending bottle match window closed message: {e}")

    triggering_player_id = current_bottle_ctx.get('triggering_player_id')
    game['bottle_match_context'] = None
    game['bottle_match_context_just_ended'] = True

    if triggering_player_id:
        game['current_player_id'] = triggering_player_id
        logger.info(f"EndBottleMatchJob: Bottle match window processing complete for triggerer {triggering_player_id}. Current player ID set. About to advance turn.")
    else:
        logger.warning(f"EndBottleMatchJob: triggering_player_id missing in bottle_context for chat {chat_id} on timeout.")

    # Call advance_turn_or_continue_sequence ONCE, passing the game object fetched by this job
    await advance_turn_or_continue_sequence(game, context)
    logger.debug(f"EndBottleMatchJob: Returned from advance_turn_or_continue_sequence for chat {chat_id}.")

async def initiate_bottle_matching_window(game_obj: dict, context: ContextTypes.DEFAULT_TYPE, discarded_bottle_card: dict):
    game = game_obj
    if not game:        
        chat_id_log = game_obj.get('chat_id', "UNKNOWN_CHAT") if isinstance(game_obj, dict) else "UNKNOWN_CHAT (game_obj is not dict)"
        logger.error(f"IBMW: No game for chat {chat_id_log}")
        return

    chat_id = game.get('chat_id')   
    if not chat_id:
        logger.error(f"IBMW: chat_id missing from game_obj. Game: {game}")
        return

    # Stale game check
    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"IBMW: Stale game_obj for C:{chat_id}. Aborting.")
        return

    if game.get('phase') == GAME_PHASES["BOTTLE_MATCHING_WINDOW"] and game.get('bottle_match_context'):
        logger.warning(f"IBMW: Attempt to re-initiate bottle matching window for C:{chat_id} while one is active. Ignoring.")
        return

    game['phase'] = GAME_PHASES["BOTTLE_MATCHING_WINDOW"]
    bottle_value_to_match = discarded_bottle_card.get('value')
    discarder_id = game.get('current_player_id') 

    job_suffix = f"{chat_id}_{int(time.time())}"
    timeout_job_name = f"bottle_match_timeout_{job_suffix}"

    current_bottle_match_context = {
        'discarded_card_value': bottle_value_to_match,
        'window_end_time': time.time() + BOTTLE_MATCH_WINDOW_SECONDS,
        'fastest_matcher_id': None,
        'timeout_job_name': timeout_job_name,
        'notified_players_pm_ids': {},
        'triggering_player_id': discarder_id,
        'failed_matchers': set(),
    }
    game['bottle_match_context'] = current_bottle_match_context
    current_bottle_ctx_signature = id(current_bottle_match_context)

    logger.info(f"IBMW: Bottle Match Window OPENED for Bottle {bottle_value_to_match} in C:{chat_id}, triggered by P:{discarder_id}. Timeout job: {timeout_job_name}")
    try:
        await context.bot.send_message(
            chat_id,
            f"🍾 Bottle card <b>{escape_html(discarded_bottle_card.get('name'))}</b> discarded! "
            f"Anyone with a matching Bottle {bottle_value_to_match} has {BOTTLE_MATCH_WINDOW_SECONDS} seconds to try and discard it facedown NOW!",
            parse_mode=ParseMode.HTML
        )
    except TelegramError as e: logger.error(f"IBMW: Error sending bottle match window open message for C:{chat_id}: {e}")

    all_active_players = game_state_manager.get_active_players_in_turn_order(chat_id)
    for p_data in all_active_players:
        logger.debug(f"IBMW: Checking player {p_data['id']} (Discarder: {discarder_id}) for bottle match prompt.")

        has_matchable_unblocked_bottle = any(
            card.get('type') == 'bottle' and card.get('value') == bottle_value_to_match and
            not (str(p_data['id']) in game.get('blocked_cards', {}) and card_idx in game['blocked_cards'][str(p_data['id'])])
            for card_idx, card in enumerate(p_data.get('hand', []))
        )

        if p_data.get('is_ai'):
            if has_matchable_unblocked_bottle and random.random() < 0.6: 
                matchable_indices = [
                    idx for idx, card in enumerate(p_data.get('hand', []))
                    if card.get('type') == 'bottle' and card.get('value') == bottle_value_to_match and
                       not (str(p_data['id']) in game.get('blocked_cards', {}) and idx in game['blocked_cards'][str(p_data['id'])])
                ]
                if matchable_indices:
                    ai_chosen_card_idx_to_match = random.choice(matchable_indices)
                    ai_delay = random.uniform(1.0, BOTTLE_MATCH_WINDOW_SECONDS - 1.0)
                    ai_job_name = f"ai_bottle_match_{p_data['id']}_{job_suffix}"
                    logger.info(f"IBMW: AI {p_data['id']} has matching bottle. Scheduling attempt for card #{ai_chosen_card_idx_to_match} in C:{chat_id} in {ai_delay:.1f}s. CtxSig: {current_bottle_ctx_signature}")
                    context.job_queue.run_once(
                        ai_attempt_bottle_match_job, ai_delay,
                        data={ 'chat_id': chat_id, 'ai_player_id': p_data['id'],
                               'ai_card_idx_to_match': ai_chosen_card_idx_to_match,
                               'expected_bottle_ctx_signature': current_bottle_ctx_signature },
                        name=ai_job_name )
            else: logger.info(f"IBMW: AI {p_data['id']} either has no matchable bottle or chose not to attempt this time in C:{chat_id}.")
        else: 
            if not p_data.get('hand'):
                continue

            pm_text = (f"⚡️ Quick! A Bottle {bottle_value_to_match} was discarded in the main game! "
                       f"If you have a matching Bottle {bottle_value_to_match}, choose it from your hand (facedown by position) to discard. You have {BOTTLE_MATCH_WINDOW_SECONDS}s!")
            
            player_blocked_indices = set()
            if str(p_data['id']) in game.get('blocked_cards', {}):
                player_blocked_indices = set(game['blocked_cards'][str(p_data['id'])].keys())

            pm_kbd = keyboards.get_bottle_match_prompt_keyboard(
                player_id=p_data['id'],
                hand=p_data['hand'],
                blocked_card_indices=player_blocked_indices
            )
            pm_player_id_for_send = int(p_data['id']) if isinstance(p_data['id'], str) and p_data['id'].isdigit() else p_data['id']
            pm_message_id = await send_message_to_player(context, pm_player_id_for_send, pm_text, reply_markup=pm_kbd)
            if pm_message_id:
                current_bottle_match_context['notified_players_pm_ids'][p_data['id']] = pm_message_id

    context.job_queue.run_once(end_bottle_match_window_job, BOTTLE_MATCH_WINDOW_SECONDS,
                               data={'chat_id': chat_id, 'expected_bottle_ctx_signature': current_bottle_ctx_signature}, 
                               name=timeout_job_name)

async def handle_player_action_draw_deck(game_obj: dict, context: ContextTypes.DEFAULT_TYPE, player_id: Union[int, str]):
    game = game_obj
    player_data = game_state_manager.get_player_by_id(game['chat_id'], player_id)
    if not game or not player_data: 
        logger.error(f"handle_player_action_draw_deck: Game or player {player_id} not found for chat {game['chat_id']}.")
        return

    logger.info(f"Player {player_id} chose to draw from deck in chat {game['chat_id']}.")

    if not game.get('deck'): 
        logger.info(f"Draw Deck: Deck empty for chat {game['chat_id']}. Attempting reshuffle.")
        if game.get('discard_pile'):
            top_discard = game['discard_pile'].pop() if game['discard_pile'] else None
            game['deck'] = game['discard_pile'][:]
            random.shuffle(game['deck'])
            game['discard_pile'] = [top_discard] if top_discard else []
            await context.bot.send_message(game['chat_id'], f"{get_player_mention(player_data)} notes the deck was empty; discard pile reshuffled.", parse_mode=ParseMode.HTML)
        if not game.get('deck'): 
            logger.warning(f"Draw Deck: Still no cards after reshuffle for chat {game['chat_id']}.")
            await send_message_to_player(context, player_id, "The deck is completely empty! You cannot draw from it.")
            # Give player turn options again, excluding draw from deck
            is_f_cycle_ac = (game['cycle_count'] == 1 and player_id == game.get('al_capone_player_id'))
            temp_kbd_btns = [[InlineKeyboardButton("🗣️ Call Omerta", callback_data=f"turn_call_omerta_{player_id}")]]
            if not is_f_cycle_ac and game.get('discard_pile') and \
               (game['discard_pile'][-1].get('type') == 'bottle' or game['discard_pile'][-1].get('name') == 'The Alibi'):
                top_d_name = game['discard_pile'][-1].get('name', 'Card')
                temp_kbd_btns.append([InlineKeyboardButton(f"♻️ Take from Discard ({top_d_name})", callback_data=f"turn_draw_discard_{player_id}")])
            if not is_f_cycle_ac and game.get('discard_pile') and game['discard_pile'][-1].get('type') == 'bottle':
                 temp_kbd_btns.append([InlineKeyboardButton(f"🍾 Match {game['discard_pile'][-1].get('name', 'Bottle')}?", callback_data=f"turn_match_discarded_bottle_{player_id}")])
            await send_message_to_player(context, player_id, "Deck empty. Choose another action:", reply_markup=InlineKeyboardMarkup(temp_kbd_btns))
            return

    drawn_card = game['deck'].pop()
    game.setdefault('player_turn_context', {}).setdefault(player_id, {})['drawn_card'] = drawn_card
    game['player_turn_context'][player_id]['draw_source'] = 'deck'
    
    blocked_indices = set(game.get('blocked_cards', {}).get(player_id, {}).keys())

    await send_message_to_player(context, player_id,
        f"You drew: <b>{escape_html(drawn_card['name'])}</b> ({drawn_card.get('points', drawn_card.get('value', '?'))} pts).\n"
        "Now, choose one of your current hand cards (facedown by position) to replace with this drawn card.",
        reply_markup=keyboards.get_card_selection_keyboard(
            purpose_prefix="replace_hand_card", player_hand=player_data['hand'], player_id_context=player_id,
            facedown=True, num_to_select=1, min_to_select=1,
            allow_cancel=True, cancel_callback_data=f"replace_hand_card_cancel_overall_{player_id}",
            blocked_card_indices=blocked_indices
        )
    )
    try:
        await context.bot.send_message(game['chat_id'], f"{get_player_mention(player_data)} drew a card from the deck and is choosing a card to replace.", parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Error sending group message for draw deck: {e}")

async def handle_player_action_draw_discard(game_obj: dict, context: ContextTypes.DEFAULT_TYPE, player_id: Union[int, str]):
    game = game_obj
    player_data = game_state_manager.get_player_by_id(game['chat_id'], player_id)
    if not game or not player_data: 
        logger.error(f"handle_player_action_draw_discard: Game or player {player_id} not found for chat {game['chat_id']}.")
        return

    logger.info(f"Player {player_id} chose to draw from discard in chat {game['chat_id']}.")

    if not game.get('discard_pile'):
        await send_message_to_player(context, player_id, "The discard pile is empty. Cannot draw from it.")
        # Resend turn options
        is_f_cycle_ac = (game['cycle_count'] == 1 and player_id == game.get('al_capone_player_id'))
        action_kbd = keyboards.get_player_turn_actions_keyboard(game, player_data, is_f_cycle_ac)
        await send_message_to_player(context, player_id, "Choose another action:", reply_markup=action_kbd)
        return

    top_discard = game['discard_pile'][-1]
    can_take_from_discard = (top_discard.get('type') == 'bottle') or \
                            (top_discard.get('type') == 'character' and top_discard.get('name') == 'The Alibi')

    if not can_take_from_discard:
        await send_message_to_player(context, player_id, f"You cannot take '{escape_html(top_discard.get('name', 'this card'))}' from the discard pile. Only Bottles or Alibi are allowed.")
        is_f_cycle_ac = (game['cycle_count'] == 1 and player_id == game.get('al_capone_player_id'))
        action_kbd = keyboards.get_player_turn_actions_keyboard(game, player_data, is_f_cycle_ac)
        await send_message_to_player(context, player_id, "Choose another action:", reply_markup=action_kbd)
        return

    drawn_card = game['discard_pile'].pop() # Take the card
    game.setdefault('player_turn_context', {}).setdefault(player_id, {})['drawn_card'] = drawn_card
    game['player_turn_context'][player_id]['draw_source'] = 'discard'
    
    blocked_indices = set(game.get('blocked_cards', {}).get(player_id, {}).keys())

    await send_message_to_player(context, player_id,
        f"You took <b>{escape_html(drawn_card['name'])}</b> ({drawn_card.get('points', drawn_card.get('value', '?'))} pts) from the discard pile.\n"
        "Now, choose one of your hand cards (facedown by position) to replace with this card.",
        reply_markup=keyboards.get_card_selection_keyboard(
            purpose_prefix="replace_hand_card", player_hand=player_data['hand'], player_id_context=player_id,
            facedown=True, num_to_select=1, min_to_select=1,
            allow_cancel=True, cancel_callback_data=f"replace_hand_card_cancel_overall_{player_id}",
            blocked_card_indices=blocked_indices
        )
    )
    try:
        await context.bot.send_message(game['chat_id'], f"{get_player_mention(player_data)} took {escape_html(drawn_card['name'])} from the discard pile and is choosing a card to replace.", parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Error sending group message for draw discard: {e}")

async def process_card_replacement(game_obj: dict, context: ContextTypes.DEFAULT_TYPE,
                                 player_id: Union[int, str],
                                 card_idx_to_replace: int,
                                 new_card: dict,
                                 source_of_draw: str):
    """Finalizes replacing a card in hand and handles the discarded old card by calling process_discarded_card."""
    game = game_obj
    if not game:
        logger.error(f"PCR: Game object is None. Player: {player_id}, NewCard: {new_card.get('name') if new_card else 'N/A'}.")
        return

    chat_id = game.get('chat_id') # Define chat_id for logging and further use
    if not chat_id:
        logger.error(f"PCR: chat_id missing from game_obj. Player: {player_id}, Game: {game}")
        return
    
    # Stale game object check
    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"PCR: Stale game_obj for C:{chat_id} passed to process_card_replacement. Aborting.")
        return

    player_data = game_state_manager.get_player_by_id(chat_id, player_id) # Uses validated chat_id
    if not player_data:
        logger.error(f"PCR: Player {player_id} not found for chat {chat_id}.")
        return

    logger.info(f"PCR: Player {player_id} replacing card #{card_idx_to_replace+1} with {new_card.get('name')} from {source_of_draw} in C:{chat_id}.")

    if not (0 <= card_idx_to_replace < len(player_data.get('hand',[]))):
        await send_message_to_player(context, player_id, "Invalid card selection for replacement. Your turn might be reset or action cancelled.")
        logger.error(f"PCR: Invalid card_idx_to_replace {card_idx_to_replace} for player {player_id} in chat {chat_id}")
        current_player_obj_for_kbd = game_state_manager.get_player_by_id(chat_id, game.get('current_player_id', player_id))
        if current_player_obj_for_kbd:
             is_f_cycle_ac = (game['cycle_count'] == 1 and current_player_obj_for_kbd['id'] == game.get('al_capone_player_id'))
             action_kbd = keyboards.get_player_turn_actions_keyboard(game, current_player_obj_for_kbd, is_f_cycle_ac)
             await send_message_to_player(context, player_id, "Error in card replacement. It's still your turn. Choose an action:", reply_markup=action_kbd)
        return

    old_card_replaced = player_data['hand'][card_idx_to_replace]
    player_data['hand'][card_idx_to_replace] = new_card
    game.setdefault('discard_pile', []).append(old_card_replaced)
    logger.debug(f"PCR: Card {old_card_replaced.get('name')} added to discard pile. Pile size: {len(game['discard_pile'])}.")

    # Player loses knowledge of the card at the replaced position if they had viewed it initially.
    if card_idx_to_replace in player_data.get('viewed_card_indices', set()):
        player_data['viewed_card_indices'].remove(card_idx_to_replace)
        logger.debug(f"PCR: Player {player_id} lost viewed status for card index {card_idx_to_replace}.")

    await send_message_to_player(context, player_id,
        f"You replaced your Card #{card_idx_to_replace+1} (which was {escape_html(old_card_replaced.get('name'))}) "
        f"with {escape_html(new_card.get('name'))}. The {escape_html(old_card_replaced.get('name'))} is now discarded."
    )

    group_msg = (f"{get_player_mention(player_data)} drew from the {source_of_draw}, "
                 f"replaced Card #{card_idx_to_replace+1} (<i>choice was facedown</i>), "
                 f"and discarded <b>{escape_html(old_card_replaced.get('name'))}</b> "
                 f"({old_card_replaced.get('points', old_card_replaced.get('value', '?'))} pts).")
    try:
        await context.bot.send_message(chat_id, group_msg, parse_mode=ParseMode.HTML) # Use validated chat_id
    except TelegramError as e: logger.error(f"PCR: Error sending group message for card replacement: {e}")

    await process_discarded_card(game, context, player_id, old_card_replaced)

async def execute_the_mole_ability(game_obj: dict, context: ContextTypes.DEFAULT_TYPE,
                                   mole_player_id: Union[int,str], card_idx_to_view: int):
    game = game_obj
    if not game: # Add robust check
        logger.error(f"ExecuteMole: game_obj is None. Player: {mole_player_id}, CardIdx: {card_idx_to_view}.")
        return
    
    chat_id = game.get('chat_id') # For logging
    if not chat_id:
        logger.error(f"ExecuteMole: chat_id missing from game_obj. Player: {mole_player_id}, Game: {game}")
        return

    # Stale game object check (important if this function can be called with a game_obj not directly from manager)
    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"ExecuteMole: Stale game_obj for C:{chat_id}. Aborting.")
        return

    mole_player = game_state_manager.get_player_by_id(chat_id, mole_player_id) 
    if not game or not mole_player: 
        logger.error(f"ExecuteMole: Game or player {mole_player_id} not found for chat {chat_id}.")
        if game: 
             game['active_ability_context'] = None
             await advance_turn_or_continue_sequence(game, context) 
        return

    logger.info(f"ExecuteMole: Executing The Mole ability for player {mole_player_id}, viewing card #{card_idx_to_view+1} in chat {chat_id}.")

    if 0 <= card_idx_to_view < len(mole_player.get('hand',[])):
        card_viewed = mole_player['hand'][card_idx_to_view]
        mole_player.setdefault('viewed_card_indices', set()).add(card_idx_to_view) 

        pm_text = (f"The Mole strikes! You peeked at your Card #{card_idx_to_view+1}: "
                   f"<b>{escape_html(card_viewed.get('name'))}</b> ({card_viewed.get('points', card_viewed.get('value', '?'))} pts).")
        group_text = f"🤫 {get_player_mention(mole_player)} (The Mole) discreetly checked one of their own cards."
        
        if not mole_player.get('is_ai'):
            await send_message_to_player(context, mole_player_id, pm_text, parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(game['chat_id'], group_text, parse_mode=ParseMode.HTML)
        except TelegramError as e: logger.error(f"Error sending Mole group message: {e}")
        pass
    else:
        logger.error(f"ExecuteMole: Invalid card index {card_idx_to_view} for The Mole by player {mole_player_id} in chat {chat_id}.")
        if not mole_player.get('is_ai'):
            await send_message_to_player(context, mole_player_id, "Error: Invalid card selected to view with The Mole.")
    
    if game: game['active_ability_context'] = None 
    await advance_turn_or_continue_sequence(game, context)

async def resume_original_ability_after_killer_interaction(game_obj: dict, context: ContextTypes.DEFAULT_TYPE,
                                                           original_ability_ctx_snapshot: dict,
                                                           killer_action_result: str): 
    game = game_obj
    
    if not game or not original_ability_ctx_snapshot:
        chat_id_log = original_ability_ctx_snapshot.get('chat_id_for_resume_log') if original_ability_ctx_snapshot else "UNKNOWN_CHAT (resume_orig_ability)"
        logger.warning(f"ROA: Cannot resume original ability for C:{chat_id_log}, game_obj or context snapshot missing.")
        if game: 
            game['active_ability_context'] = None 
            await advance_turn_or_continue_sequence(game, context) 
        return
    
    chat_id = game.get('chat_id') 
    if not chat_id:
        logger.error(f"ROA: chat_id missing from game_obj. Snapshot: {original_ability_ctx_snapshot}, KillerRes: {killer_action_result}")
        return

    # Stale game object check
    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"ROA: Stale game_obj for C:{chat_id} passed to resume_original_ability. Aborting.")
        return

    # Restore the original ability's context from the snapshot
    game['active_ability_context'] = original_ability_ctx_snapshot

    resuming_ability_ctx = game['active_ability_context'] 
    original_ability_name = resuming_ability_ctx.get('card_name')
    original_player_id = resuming_ability_ctx.get('player_id')
    original_step = resuming_ability_ctx.get('step')
    original_player_obj = game_state_manager.get_player_by_id(chat_id, original_player_id)

    if not (original_ability_name and original_player_id and original_step and original_player_obj):
        logger.error(f"ROA: Resuming original ability failed: incomplete data in snapshot for C:{chat_id}. Snapshot: {original_ability_ctx_snapshot}")
        game['active_ability_context'] = None
        await advance_turn_or_continue_sequence(game, context)
        return

    logger.info(f"ROA: Resuming ability '{original_ability_name}' for P:{original_player_id} in C:{chat_id} from step '{original_step}' after Killer was {killer_action_result}.")

    original_card_template = next((c for c in CHARACTER_CARDS if c['name'] == original_ability_name), None)
    original_ability_time = original_card_template.get('ability_time', 20) if original_card_template else 20
    
    def reschedule_original_timeout_for_resumed_step():
        new_timeout_job_name = f"ability_timeout_RESUMED_{chat_id}_{original_player_id}_{original_ability_name.replace(' ','')}_{int(time.time())}"
        resuming_ability_ctx['timeout_job_name'] = new_timeout_job_name
        context.job_queue.run_once(character_ability_timeout_job, original_ability_time,
                                   data={'chat_id': chat_id, 'expected_context_signature': id(resuming_ability_ctx)},
                                   name=new_timeout_job_name)
        logger.info(f"ROA: Rescheduled timeout for resumed '{original_ability_name}' (C:{chat_id}, step: {original_step}, job: {new_timeout_job_name}).")

    # --- Specific Resumption Logic ---
    if "_confirm_target" in original_step or "_confirm_swap" in original_step or "_confirmed_block_target" in original_step:
        logger.info(f"ROA: Resuming '{original_ability_name}' for C:{chat_id}: was at final confirmation step '{original_step}'. Proceeding to execute.")
        if original_ability_name == "The Lady" and resuming_ability_ctx.get('targets_chosen'):
            await execute_the_lady_ability(game, context, original_player_id, resuming_ability_ctx['targets_chosen'][0])
        elif original_ability_name == "The Mamma" and resuming_ability_ctx.get('targets_chosen'):
            await execute_the_mamma_ability(game, context, original_player_id, resuming_ability_ctx['targets_chosen'][0])
        elif original_ability_name == "The Snitch" and resuming_ability_ctx.get('targets_chosen'):
            await execute_the_snitch_ability(game, context, original_player_id, resuming_ability_ctx['targets_chosen'])
        elif original_ability_name == "Police Patrol" and resuming_ability_ctx.get('targets_chosen') and resuming_ability_ctx.get('cards_selected_indices'):
            await execute_police_patrol_ability(game, context, original_player_id, resuming_ability_ctx['targets_chosen'][0], resuming_ability_ctx['cards_selected_indices'][0])
        elif original_ability_name == "The Gangster" and \
             resuming_ability_ctx.get('p1_id') and resuming_ability_ctx.get('p1_card_idx') is not None and \
             resuming_ability_ctx.get('p2_id') and resuming_ability_ctx.get('p2_card_idx') is not None:
            swap_details = {'type': resuming_ability_ctx.get('gangster_swap_type'),
                            'p1_id': resuming_ability_ctx['p1_id'], 'p1_card_idx': resuming_ability_ctx['p1_card_idx'],
                            'p2_id': resuming_ability_ctx['p2_id'], 'p2_card_idx': resuming_ability_ctx['p2_card_idx']}
            await execute_gangster_swap(game, context, original_player_id, swap_details)
        else:
            logger.warning(f"ROA: Resuming '{original_ability_name}' for C:{chat_id}: Don't know how to auto-execute from step '{original_step}'. Ability may fizzle for {original_player_id}.")
            await send_message_to_player(context, original_player_id, f"Resuming {original_ability_name}: Flow was interrupted. Please try ability again if turn allows, or it may fizzle.")
            game['active_ability_context'] = None
            await advance_turn_or_continue_sequence(game, context)
    
    elif original_step.endswith("_select_target") or original_step.endswith("_select_targets") or \
         original_step.endswith("_select_own_card") or original_step.endswith("_select_opponent_card") or \
         original_step.startswith("gangster_others_select_"):

        logger.info(f"ROA: Resuming '{original_ability_name}' for C:{chat_id}: was at selection step '{original_step}'. Re-prompting player {original_player_id}.")        
        await send_message_to_player(context, original_player_id, f"Resuming your {original_ability_name} ability after an interruption. Please make your selection again.")
        
        if original_ability_name == "The Lady":
            resuming_ability_ctx['step'] = 'lady_select_target'; resuming_ability_ctx['targets_chosen'] = []
            kbd = keyboards.get_target_player_keyboard(game, original_player_id, "ability_lady_target", 1,1,None,None,f"ability_lady_overall_cancel_{original_player_id}")
            await send_message_to_player(context, original_player_id, "Resuming The Lady: Choose opponent to shuffle:", reply_markup=kbd)
        elif original_ability_name == "The Mamma":
            resuming_ability_ctx['step'] = 'mamma_select_target'; resuming_ability_ctx['targets_chosen'] = []
            kbd = keyboards.get_target_player_keyboard(game, original_player_id, "ability_mamma_target", 1,1,None,None,f"ability_mamma_overall_cancel_{original_player_id}")
            await send_message_to_player(context, original_player_id, "Resuming The Mamma: Choose player to skip:", reply_markup=kbd)
        elif original_ability_name == "The Snitch":
            resuming_ability_ctx['step'] = 'snitch_select_targets'; resuming_ability_ctx['targets_chosen'] = []
            kbd = keyboards.get_target_player_keyboard(game, original_player_id, "ability_snitch_target", 2,1,None,None,f"ability_snitch_overall_cancel_{original_player_id}")
            await send_message_to_player(context, original_player_id, "Resuming The Snitch: Choose 1 or 2 players:", reply_markup=kbd)
        elif original_ability_name == "Police Patrol":
            resuming_ability_ctx['step'] = 'police_select_target_player'; resuming_ability_ctx['targets_chosen'] = []; resuming_ability_ctx['cards_selected_indices'] = []
            kbd = keyboards.get_target_player_keyboard(game, original_player_id, "ability_police_player", 1,1,None,None,f"ability_police_overall_cancel_{original_player_id}")
            await send_message_to_player(context, original_player_id, "Resuming Police Patrol: Choose opponent to target:", reply_markup=kbd)
        elif original_ability_name == "The Gangster":
            resuming_ability_ctx['step'] = 'gangster_select_action_type'
            for key_to_clear in ['gangster_swap_type', 'p1_id', 'p1_card_idx', 'p2_id', 'p2_card_idx', 'targets_chosen', 'cards_selected_indices']:
                if key_to_clear in resuming_ability_ctx: del resuming_ability_ctx[key_to_clear]
            kbd = keyboards.get_gangster_action_type_keyboard(original_player_id)
            await send_message_to_player(context, original_player_id, "Resuming The Gangster: What's your play?", reply_markup=kbd)
        else: 
            logger.warning(f"ROA: Resuming '{original_ability_name}' for C:{chat_id}: No specific re-prompt for step '{original_step}'. Ability may fizzle for {original_player_id}.")
            await send_message_to_player(context, original_player_id, f"Your {original_ability_name} action was interrupted. It may not complete as expected.")
            game['active_ability_context'] = None; await advance_turn_or_continue_sequence(game, context); return

        reschedule_original_timeout_for_resumed_step()
    
    else:
        logger.error(f"ROA: CRITICAL FALLBACK in Killer Resumption for C:{chat_id}: Unhandled state for '{original_ability_name}' at step '{original_step}'. P:{original_player_id}'s ability fizzles.")
        await send_message_to_player(context, original_player_id, f"Your {original_ability_name} action was interrupted and could not be resumed from its current state. The effect is lost.")
        game['active_ability_context'] = None
        game['current_player_id'] = original_player_id
        await advance_turn_or_continue_sequence(game, context)

async def check_for_killer_reaction(chat_id: int, context: ContextTypes.DEFAULT_TYPE,
                                    original_ability_user_id: Union[int,str],
                                    player_being_targeted_id: Union[int,str],
                                    original_ability_name: str,
                                    original_ability_context_signature_at_snapshot: int # id() of the original ability's context
                                    ) -> bool: # Returns True if Killer interaction is initiated
    game = game_state_manager.get_game(chat_id)
    if not game: logger.error(f"KillerCheck: No game for chat {chat_id}"); return False

    player_being_targeted = game_state_manager.get_player_by_id(chat_id, player_being_targeted_id)

    original_ability_full_context = game.get('active_ability_context')
    if not original_ability_full_context or id(original_ability_full_context) != original_ability_context_signature_at_snapshot:
        logger.error(f"KillerCheck: Original ability context signature mismatch for {original_ability_name}. Expected sig: {original_ability_context_signature_at_snapshot}, game has {id(original_ability_full_context) if original_ability_full_context else 'None'}. Cannot proceed.")
        return False

    if not player_being_targeted or player_being_targeted.get('is_ai'): # AI doesn't use Killer (for now)
        logger.debug(f"KillerCheck: Player {player_being_targeted_id} is AI or not found. No Killer reaction.")
        return False

    logger.info(f"Player {player_being_targeted_id} is targeted by '{original_ability_name}'. Initiating Killer prompt (player will be prompted even if they don't have the card).")

    if original_ability_full_context.get('timeout_job_name'): # Check if job name exists
        cancel_job(context, original_ability_full_context['timeout_job_name'])
        original_ability_full_context['timeout_job_name'] = None # Mark as cancelled in the snapshot too
    else:
        logger.warning(f"KillerCheck: Original ability context for '{original_ability_name}' had no timeout_job_name to cancel.")


    killer_card_template = next((c for c in CHARACTER_CARDS if c['name'] == 'The Killer'), {'ability_time': 10})
    killer_ability_time = killer_card_template.get('ability_time', 10)

    killer_job_suffix = f"{chat_id}_{player_being_targeted_id}_killer_react_{int(time.time())}"
    killer_timeout_job_name = f"killer_react_timeout_{killer_job_suffix}"

    snapshotted_original_context = copy.deepcopy(original_ability_full_context)

    game['active_ability_context'] = {
        'player_id': player_being_targeted_id,
        'card_name': "The Killer",
        'step': 'killer_prompt_for_use',
        'timeout_job_name': killer_timeout_job_name,
        'original_ability_context_snapshot': snapshotted_original_context,
    }
    current_killer_context_signature = id(game['active_ability_context'])

    original_ability_user_obj = game_state_manager.get_player_by_id(chat_id, original_ability_user_id)
    target_mention = get_player_mention(original_ability_user_obj) if original_ability_user_obj else "Someone"
    
    prompt_text = (f"{target_mention} is using <b>{escape_html(original_ability_name)}</b> on you! "
                   f"Do you want to try and use 'The Killer' to counter this? ({killer_ability_time}s to decide)")

    pm_msg_id = await send_message_to_player(context, player_being_targeted_id, prompt_text,
        reply_markup=keyboards.get_killer_prompt_keyboard(player_being_targeted_id, current_killer_context_signature),
        parse_mode=ParseMode.HTML )
    if pm_msg_id:
        game['active_ability_context']['killer_prompt_message_id'] = pm_msg_id

    context.job_queue.run_once(character_ability_timeout_job, killer_ability_time,
                               data={'chat_id': chat_id, 'expected_context_signature': current_killer_context_signature},
                               name=killer_timeout_job_name)

    try:
        group_target_mention = get_player_mention(player_being_targeted) if player_being_targeted else "A player"
        await context.bot.send_message(chat_id, f"{group_target_mention} is being targeted by {escape_html(original_ability_name)} from {target_mention}. They might have a trick up their sleeve...", parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Error sending Killer check group message: {e}")

    return True 

async def execute_the_lady_ability(game_obj: dict, context: ContextTypes.DEFAULT_TYPE,
                                   lady_player_id: Union[int, str],
                                   target_player_id: Union[int, str]):
    game = game_obj
    if not game:
        logger.error(f"ExecLady: game_obj is None. Lady: {lady_player_id}, Target: {target_player_id}.")
        return

    chat_id = game.get('chat_id')
    if not chat_id:
        logger.error(f"ExecLady: chat_id missing from game_obj. Lady: {lady_player_id}, Target: {target_player_id}, Game: {game}")
        return

    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"ExecLady: Stale game_obj for C:{chat_id}. Aborting.")
        return

    lady_player = game_state_manager.get_player_by_id(chat_id, lady_player_id)
    target_player = game_state_manager.get_player_by_id(chat_id, target_player_id)

    if not lady_player or not target_player:
        logger.error(f"ExecLady: Lady ({lady_player_id}) or Target ({target_player_id}) not found in C:{chat_id}.")
        if game: game['active_ability_context'] = None
        await advance_turn_or_continue_sequence(game, context)
        return

    logger.info(f"ExecLady: Player {lady_player_id} uses The Lady on {target_player_id} in C:{chat_id}.")

    if target_player.get('hand'):
        random.shuffle(target_player['hand'])
        target_player['viewed_card_indices'] = set() # Target loses knowledge of their hand
        logger.info(f"ExecLady: Target {target_player_id}'s hand (size {len(target_player['hand'])}) shuffled by The Lady in C:{chat_id}.")
        group_msg = f"💃 {get_player_mention(lady_player)} (The Lady) has stirred things up! {get_player_mention(target_player)}'s hand has been shuffled!"
        pm_msg_target = "The Lady paid you a visit! Your hand has been shuffled, and you no longer know which card is which."

        try:
            await context.bot.send_message(chat_id, group_msg, parse_mode=ParseMode.HTML)
            if not target_player.get('is_ai'):
                await send_message_to_player(context, target_player['id'], pm_msg_target)
        except TelegramError as e:
            logger.error(f"ExecLady: Error sending Lady messages for C:{chat_id}: {e}")
    else:
        logger.info(f"ExecLady: Target {target_player_id} had no hand to shuffle in C:{chat_id}.")
        try:
            await context.bot.send_message(chat_id, f"{get_player_mention(lady_player)} (The Lady) tried to shuffle, but {get_player_mention(target_player)} had no cards!", parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.error(f"ExecLady: Error sending no-hand message for C:{chat_id}: {e}")

    if game: game['active_ability_context'] = None
    await advance_turn_or_continue_sequence(game, context)

async def execute_the_mamma_ability(game_obj: dict, context: ContextTypes.DEFAULT_TYPE,
                                    mamma_player_id: Union[int, str],
                                    target_player_id: Union[int, str]):
    game = game_obj
    if not game:
        logger.error(f"ExecMamma: game_obj is None. Mamma: {mamma_player_id}, Target: {target_player_id}.")
        return

    chat_id = game.get('chat_id')
    if not chat_id:
        logger.error(f"ExecMamma: chat_id missing from game_obj. Mamma: {mamma_player_id}, Target: {target_player_id}, Game: {game}")
        return

    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"ExecMamma: Stale game_obj for C:{chat_id}. Aborting.")
        return

    mamma_player = game_state_manager.get_player_by_id(chat_id, mamma_player_id)
    target_player = game_state_manager.get_player_by_id(chat_id, target_player_id)

    if not mamma_player or not target_player:
        logger.error(f"ExecMamma: Mamma ({mamma_player_id}) or Target ({target_player_id}) not found in C:{chat_id}.")
        if game: game['active_ability_context'] = None
        await advance_turn_or_continue_sequence(game, context)
        return

    logger.info(f"ExecMamma: Player {mamma_player_id} uses The Mamma on {target_player_id} in C:{chat_id}.")

    target_player['status'] = PLAYER_STATES["SKIPPED_TURN"]
    target_player['cannot_call_omerta'] = True # For one turn cycle
    logger.info(f"ExecMamma: Target {target_player_id} status set to SKIPPED_TURN and cannot_call_omerta=True in C:{chat_id}.")

    group_msg = f"👵 Mamma {get_player_mention(mamma_player)} lays down the law! {get_player_mention(target_player)} must skip their next turn and cannot call Omerta during it."
    pm_msg_target = "Mamma has spoken! You must skip your next turn and cannot call Omerta. Don't cross the Mamma!"

    try:
        await context.bot.send_message(chat_id, group_msg, parse_mode=ParseMode.HTML)
        if not target_player.get('is_ai'):
            await send_message_to_player(context, target_player['id'], pm_msg_target)
    except TelegramError as e:
        logger.error(f"ExecMamma: Error sending Mamma messages for C:{chat_id}: {e}")

    if game: game['active_ability_context'] = None
    await advance_turn_or_continue_sequence(game, context)

async def execute_the_snitch_ability(game_obj: dict, context: ContextTypes.DEFAULT_TYPE,
                                     snitch_player_id: Union[int, str],
                                     target_player_ids: List[Union[int, str]]):
    game = game_obj
    if not game:
        logger.error(f"ExecSnitch: game_obj is None. Snitch: {snitch_player_id}, Targets: {target_player_ids}.")
        return

    chat_id = game.get('chat_id')
    if not chat_id:
        logger.error(f"ExecSnitch: chat_id missing from game_obj. Snitch: {snitch_player_id}, Targets: {target_player_ids}, Game: {game}")
        return

    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"ExecSnitch: Stale game_obj for C:{chat_id}. Aborting.")
        return

    snitch_player = game_state_manager.get_player_by_id(chat_id, snitch_player_id)
    if not snitch_player:
        logger.error(f"ExecSnitch: Snitch player {snitch_player_id} not found in C:{chat_id}.")
        if game: game['active_ability_context'] = None
        await advance_turn_or_continue_sequence(game, context)
        return

    logger.info(f"ExecSnitch: Player {snitch_player_id} uses The Snitch on {target_player_ids} in C:{chat_id}.")
    cards_given_count = 0
    target_mentions_list = []

    for target_id in target_player_ids:
        target_player = game_state_manager.get_player_by_id(chat_id, target_id)
        if not target_player:
            logger.warning(f"ExecSnitch: Target {target_id} not found for Snitch in C:{chat_id}. Skipping.")
            continue

        target_mentions_list.append(get_player_mention(target_player))
        if game.get('deck'):
            card_to_give = game['deck'].pop()
            target_player.setdefault('hand', []).append(card_to_give)
            cards_given_count += 1
            logger.info(f"ExecSnitch: Snitch gave card {card_to_give.get('name')} to {target_id} in C:{chat_id}.")
            
            # --- THIS IS THE CHANGED LINE ---
            pm_msg_target = "The Snitch slipped you an unknown card. It has been added to your hand."
            # --- END OF CHANGE ---

            if not target_player.get('is_ai'):
                await send_message_to_player(context, target_player['id'], pm_msg_target, parse_mode=ParseMode.HTML)
        else:
            logger.warning(f"ExecSnitch: Deck empty. Cannot give card to {target_id} in C:{chat_id}.")
            pm_msg_target_no_card = "The Snitch tried to slip you a card, but the deck is empty!"
            if not target_player.get('is_ai'):
                 await send_message_to_player(context, target_player['id'], pm_msg_target_no_card)
            break

    target_mentions_str = ", ".join(target_mentions_list) if target_mentions_list else "nobody"
    group_msg = f"🗣️ {get_player_mention(snitch_player)} (The Snitch) 'shared information' (gave {cards_given_count} card(s) from the deck) with {target_mentions_str}."
    if cards_given_count == 0 and target_player_ids:
        group_msg += " But the deck was empty!"

    try:
        await context.bot.send_message(chat_id, group_msg, parse_mode=ParseMode.HTML)
    except TelegramError as e:
        logger.error(f"ExecSnitch: Error sending Snitch group message for C:{chat_id}: {e}")

    if game: game['active_ability_context'] = None
    await advance_turn_or_continue_sequence(game, context)

async def execute_police_patrol_ability(game_obj: dict, context: ContextTypes.DEFAULT_TYPE,
                                        police_player_id: Union[int, str],
                                        target_player_id: Union[int, str],
                                        target_card_idx: int):
    game = game_obj
    if not game:
        logger.error(f"ExecPolice: game_obj is None. Police: {police_player_id}, Target: {target_player_id}, CardIdx: {target_card_idx}.")
        return

    chat_id = game.get('chat_id')
    if not chat_id:
        logger.error(f"ExecPolice: chat_id missing from game_obj. Police: {police_player_id}, Target: {target_player_id}, CardIdx: {target_card_idx}, Game: {game}")
        return

    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"ExecPolice: Stale game_obj for C:{chat_id}. Aborting.")
        return

    police_player = game_state_manager.get_player_by_id(chat_id, police_player_id)
    target_player = game_state_manager.get_player_by_id(chat_id, target_player_id)

    if not police_player or not target_player:
        logger.error(f"ExecPolice: Police ({police_player_id}) or Target ({target_player_id}) not found in C:{chat_id}.")
        if game: game['active_ability_context'] = None
        await advance_turn_or_continue_sequence(game, context)
        return

    logger.info(f"ExecPolice: Player {police_player_id} uses Police Patrol on P:{target_player_id} CardIdx:{target_card_idx} in C:{chat_id}.")

    if target_player.get('hand') and 0 <= target_card_idx < len(target_player['hand']):
        # Store block: game['blocked_cards'][target_player_id_str][card_idx] = cycles_left
        # Player ID needs to be string for dictionary key if it's sometimes int
        target_player_id_str = str(target_player_id)
        game.setdefault('blocked_cards', {}).setdefault(target_player_id_str, {})[target_card_idx] = 2 # Block for 2 full cycles

        card_name_blocked = "Unknown Card"
        try: card_name_blocked = target_player['hand'][target_card_idx]['name']
        except: pass

        logger.info(f"ExecPolice: Card #{target_card_idx+1} of player {target_player_id} blocked for 2 cycles in C:{chat_id}.")
        group_msg = (f"🚨 {get_player_mention(police_player)} (Police Patrol) is on the scene! "
                     f"Card at Position #{target_card_idx+1} of {get_player_mention(target_player)} is now BLOCKED for 2 cycles.")
        pm_msg_target = (f"Police Patrol has blocked your Card at Position #{target_card_idx+1} (currently {escape_html(card_name_blocked)})! "
                         f"It cannot be selected for replacement or abilities for 2 cycles.")

        try:
            await context.bot.send_message(chat_id, group_msg, parse_mode=ParseMode.HTML)
            if not target_player.get('is_ai'):
                await send_message_to_player(context, target_player['id'], pm_msg_target, parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.error(f"ExecPolice: Error sending Police messages for C:{chat_id}: {e}")
    else:
        logger.warning(f"ExecPolice: Target {target_player_id} had no card at index {target_card_idx} to block in C:{chat_id}.")
        try:
            await context.bot.send_message(chat_id, f"{get_player_mention(police_player)} (Police Patrol) arrived, but {get_player_mention(target_player)} had no card at the chosen position!", parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.error(f"ExecPolice: Error sending no-card-to-block message for C:{chat_id}: {e}")

    if game: game['active_ability_context'] = None
    await advance_turn_or_continue_sequence(game, context)

async def execute_gangster_swap(game_obj: dict, context: ContextTypes.DEFAULT_TYPE,
                                gangster_player_id: Union[int, str],
                                swap_details: dict):
    game = game_obj
    if not game:
        logger.error(f"ExecGangster: game_obj is None. Gangster: {gangster_player_id}, Details: {swap_details}.")
        return

    chat_id = game.get('chat_id')
    if not chat_id:
        logger.error(f"ExecGangster: chat_id missing. Gangster: {gangster_player_id}, Details: {swap_details}, Game: {game}")
        return

    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"ExecGangster: Stale game_obj for C:{chat_id}. Aborting.")
        return

    gangster_player = game_state_manager.get_player_by_id(chat_id, gangster_player_id)
    p1 = game_state_manager.get_player_by_id(chat_id, swap_details.get('p1_id'))
    p2 = game_state_manager.get_player_by_id(chat_id, swap_details.get('p2_id'))
    p1_idx = swap_details.get('p1_card_idx')
    p2_idx = swap_details.get('p2_card_idx')

    if not gangster_player or not p1 or not p2 or p1_idx is None or p2_idx is None:
        logger.error(f"ExecGangster: Invalid swap details or players not found. Details: {swap_details}")
        if game: game['active_ability_context'] = None
        await advance_turn_or_continue_sequence(game, context)
        return

    logger.info(f"ExecGangster: Player {gangster_player_id} uses The Gangster. Swapping P1({p1['id']})C#{p1_idx+1} with P2({p2['id']})C#{p2_idx+1}.")

    if p1.get('hand') and p2.get('hand') and 0 <= p1_idx < len(p1['hand']) and 0 <= p2_idx < len(p2['hand']):
        # Perform the swap
        card_from_p1 = p1['hand'][p1_idx]
        card_from_p2 = p2['hand'][p2_idx]
        p1['hand'][p1_idx] = card_from_p2
        p2['hand'][p2_idx] = card_from_p1

        # Update viewed status
        if swap_details.get('type') == 'own_vs_other':
            if p1['id'] == gangster_player_id: p1.setdefault('viewed_card_indices', set()).add(p1_idx)
            elif p2['id'] == gangster_player_id: p2.setdefault('viewed_card_indices', set()).add(p2_idx)
        else: # other_vs_other
            if p1_idx in p1.get('viewed_card_indices', set()): p1['viewed_card_indices'].remove(p1_idx)
            if p2_idx in p2.get('viewed_card_indices', set()): p2['viewed_card_indices'].remove(p2_idx)

        # Send notifications about the successful swap
        group_msg = (f"🤝 {get_player_mention(gangster_player)} (The Gangster) made a deal! "
                     f"A card from {get_player_mention(p1)} (Pos #{p1_idx+1}) was swapped with "
                     f"a card from {get_player_mention(p2)} (Pos #{p2_idx+1}).")
        try:
            await context.bot.send_message(chat_id, group_msg, parse_mode=ParseMode.HTML)
            # (PMs to targets are omitted for brevity, but would go here)
        except TelegramError as e: logger.error(f"ExecGangster: Error sending messages: {e}")
    else:
        logger.warning(f"ExecGangster: Invalid card indices for swap. P1 hand: {len(p1.get('hand',[]))}, P2 hand: {len(p2.get('hand',[]))}")
        try: await context.bot.send_message(chat_id, f"{get_player_mention(gangster_player)}'s swap failed.", parse_mode=ParseMode.HTML)
        except TelegramError: pass

    # --- NEW LOGIC: Check swap count and decide next step ---
    ability_ctx = game.get('active_ability_context')
    if ability_ctx:
        ability_ctx['swap_count'] = ability_ctx.get('swap_count', 0) + 1
        logger.info(f"Gangster swap count for player {gangster_player_id} is now {ability_ctx['swap_count']}.")

        if ability_ctx['swap_count'] < 2:
            # First swap is done, re-prompt for the second.
            # Reset context keys for the next swap selection.
            for key in ['gangster_swap_type', 'p1_id', 'p1_card_idx', 'p2_id', 'p2_card_idx']:
                ability_ctx.pop(key, None)
            ability_ctx['step'] = 'gangster_select_action_type'

            kbd = keyboards.get_gangster_action_type_keyboard(gangster_player_id)
            await send_message_to_player(context, gangster_player_id,
                                         "First swap complete. You can perform one more. Choose your second action:",
                                         reply_markup=kbd)
        else:
            # Second swap is done, end the turn.
            logger.info(f"Gangster ability finished for {gangster_player_id} after 2 swaps.")
            game['active_ability_context'] = None
            await advance_turn_or_continue_sequence(game, context)
    else:
        # Fallback if context is somehow lost.
        logger.error("ExecGangster: active_ability_context was lost. Advancing turn to prevent stall.")
        await advance_turn_or_continue_sequence(game, context)

async def execute_the_driver_ability(game_obj: dict, context: ContextTypes.DEFAULT_TYPE,
                                     driver_player_id: Union[int, str],
                                     card_indices_to_discard: List[int]):
    game = game_obj
    if not game:
        logger.error(f"ExecDriver: game_obj is None. Driver: {driver_player_id}, Indices: {card_indices_to_discard}.")
        return

    chat_id = game.get('chat_id')
    if not chat_id:
        logger.error(f"ExecDriver: chat_id missing. Driver: {driver_player_id}, Indices: {card_indices_to_discard}, Game: {game}")
        return

    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"ExecDriver: Stale game_obj for C:{chat_id}. Aborting.")
        return

    driver_player = game_state_manager.get_player_by_id(chat_id, driver_player_id)
    if not driver_player:
        logger.error(f"ExecDriver: Driver player {driver_player_id} not found in C:{chat_id}.")
        if game: game['active_ability_context'] = None
        await advance_turn_or_continue_sequence(game, context)
        return

    logger.info(f"ExecDriver: Player {driver_player_id} uses The Driver, attempting to discard cards at indices {card_indices_to_discard} in C:{chat_id}.")

    # --- NEW LOGIC ---
    discarded_bottles_count = 0
    returned_non_bottles_count = 0
    successfully_discarded_names = []
    returned_card_names = []
    
    # We need a copy of the hand to iterate over while modifying the original
    original_hand = list(driver_player['hand'])
    cards_to_put_back = []
    # Sort indices in reverse to pop correctly without messing up subsequent indices
    for idx in sorted(card_indices_to_discard, reverse=True):
        if 0 <= idx < len(driver_player['hand']):
            card_to_check = driver_player['hand'].pop(idx)
            
            if card_to_check.get('type') == 'bottle':
                discarded_bottles_count += 1
                successfully_discarded_names.append(escape_html(card_to_check.get('name')))
                game.setdefault('discard_pile', []).append(card_to_check)
            else:
                # This is the key change: non-bottles are put aside to be returned
                returned_non_bottles_count += 1
                returned_card_names.append(escape_html(card_to_check.get('name')))
                cards_to_put_back.append(card_to_check)

            if idx in driver_player.get('viewed_card_indices', set()):
                driver_player['viewed_card_indices'].remove(idx)

    # Now, add the incorrect cards back to the player's hand
    if cards_to_put_back:
        driver_player['hand'].extend(cards_to_put_back)
        # We can shuffle the hand here so the player doesn't know where the returned cards are
        random.shuffle(driver_player['hand'])
        driver_player['viewed_card_indices'] = set() # Player loses all knowledge of their hand

    # Apply penalty for each non-bottle
    penalty_cards_drawn = 0
    if returned_non_bottles_count > 0:
        for _ in range(returned_non_bottles_count):
            if game.get('deck'):
                penalty_card = game['deck'].pop()
                driver_player.setdefault('hand', []).append(penalty_card)
                penalty_cards_drawn += 1
            else:
                logger.warning(f"ExecDriver: Deck empty, cannot draw penalty card for Driver.")
                break

    group_msg_parts = [f"🚗 {get_player_mention(driver_player)} (The Driver) made a drop!"]
    if discarded_bottles_count > 0:
        group_msg_parts.append(f"Successfully discarded {discarded_bottles_count} Bottle(s): {', '.join(successfully_discarded_names)}.")
    if returned_non_bottles_count > 0:
        group_msg_parts.append(f"Tried to discard {returned_non_bottles_count} non-Bottle(s) ({', '.join(returned_card_names)}), which were returned to their hand.")
    if penalty_cards_drawn > 0:
        group_msg_parts.append(f"They receive {penalty_cards_drawn} penalty card(s) from the deck.")

    pm_msg_driver = "<b>Driver Action Summary:</b>\n"
    pm_msg_driver += f"- Successfully discarded bottles: {discarded_bottles_count}\n"
    if returned_non_bottles_count > 0:
        pm_msg_driver += f"- Incorrectly chosen cards returned to hand: {returned_non_bottles_count}\n"
    if penalty_cards_drawn > 0:
        pm_msg_driver += f"- Penalty cards drawn from deck: {penalty_cards_drawn}\n"
    
    # Final state message
    pm_msg_driver += f"\nYour hand was shuffled. You no longer know which card is which."

    try:
        await context.bot.send_message(chat_id, " ".join(group_msg_parts), parse_mode=ParseMode.HTML)
        if not driver_player.get('is_ai'):
            await send_message_to_player(context, driver_player_id, pm_msg_driver, parse_mode=ParseMode.HTML)
    except TelegramError as e:
        logger.error(f"ExecDriver: Error sending Driver messages for C:{chat_id}: {e}")

    if game: game['active_ability_context'] = None
    await advance_turn_or_continue_sequence(game, context)

async def execute_safecracker_exchange(game_obj: dict, context: ContextTypes.DEFAULT_TYPE,
                                       safecracker_player_id: Union[int, str],
                                       safe_card_idx_to_take: int,
                                       hand_card_idx_to_give: int):
    game = game_obj
    if not game:
        logger.error(f"ExecSafe: game_obj is None. Player: {safecracker_player_id}, SafeIdx: {safe_card_idx_to_take}, HandIdx: {hand_card_idx_to_give}.")
        return

    chat_id = game.get('chat_id')
    if not chat_id:
        logger.error(f"ExecSafe: chat_id missing. Player: {safecracker_player_id}, SafeIdx: {safe_card_idx_to_take}, HandIdx: {hand_card_idx_to_give}, Game: {game}")
        return

    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"ExecSafe: Stale game_obj for C:{chat_id}. Aborting.")
        return

    safecracker_player = game_state_manager.get_player_by_id(chat_id, safecracker_player_id)
    if not safecracker_player:
        logger.error(f"ExecSafe: Safecracker player {safecracker_player_id} not found in C:{chat_id}.")
        if game: game['active_ability_context'] = None
        await advance_turn_or_continue_sequence(game, context)
        return

    logger.info(f"ExecSafe: Player {safecracker_player_id} uses Safecracker. Taking Safe#{safe_card_idx_to_take+1}, Giving Hand#{hand_card_idx_to_give+1} in C:{chat_id}.")

    valid_exchange = False
    if safecracker_player.get('hand') and game.get('safe') and \
       0 <= hand_card_idx_to_give < len(safecracker_player['hand']) and \
       0 <= safe_card_idx_to_take < len(game['safe']):

        card_from_hand = safecracker_player['hand'][hand_card_idx_to_give]
        card_from_safe = game['safe'][safe_card_idx_to_take]

        safecracker_player['hand'][hand_card_idx_to_give] = card_from_safe
        game['safe'][safe_card_idx_to_take] = card_from_hand
        valid_exchange = True

        # Safecracker "views" the card they took from the safe and put into their hand
        safecracker_player.setdefault('viewed_card_indices', set()).add(hand_card_idx_to_give)
        # They lose knowledge of the card they put into the safe if they had viewed it
        # (This is less relevant as Safe cards aren't "viewed" by position, but for consistency)

        logger.info(f"ExecSafe: Exchange successful in C:{chat_id}. Player Hand#{hand_card_idx_to_give+1} is now {card_from_safe.get('name')}. Safe#{safe_card_idx_to_take+1} is now {card_from_hand.get('name')}.")
        group_msg = (f"💰 {get_player_mention(safecracker_player)} (The Safecracker) made an exchange with the Safe! "
                     f"One card from their hand (Position #{hand_card_idx_to_give+1}) was swapped with a card from the Safe (Position #{safe_card_idx_to_take+1}).")
        pm_msg_player = (f"Safecracker successful! You swapped your Card at Position #{hand_card_idx_to_give+1} "
                         f"(which was {escape_html(card_from_hand.get('name'))}) with the card from Safe Position #{safe_card_idx_to_take+1}. "
                         f"You received: <b>{escape_html(card_from_safe.get('name'))}</b> ({card_from_safe.get('points','?')} pts).")

        try:
            await context.bot.send_message(chat_id, group_msg, parse_mode=ParseMode.HTML)
            if not safecracker_player.get('is_ai'):
                await send_message_to_player(context, safecracker_player_id, pm_msg_player, parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.error(f"ExecSafe: Error sending Safecracker messages for C:{chat_id}: {e}")
    else:
        logger.warning(f"ExecSafe: Invalid indices or empty hand/safe for exchange in C:{chat_id}.")
        try:
            await context.bot.send_message(chat_id, f"{get_player_mention(safecracker_player)} (The Safecracker) fumbled the exchange!", parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.error(f"ExecSafe: Error sending failed exchange message for C:{chat_id}: {e}")

    if game: game['active_ability_context'] = None
    await advance_turn_or_continue_sequence(game, context)

async def initiate_character_ability(game_obj: dict, context: ContextTypes.DEFAULT_TYPE,
                                   player_id_who_discarded: Union[int,str], discarded_card: dict):
    game = game_obj
    if not game:
        p_id_log = player_id_who_discarded if player_id_who_discarded else "UNKNOWN_PLAYER"
        card_name_log = discarded_card.get('name') if discarded_card and isinstance(discarded_card, dict) else "UNKNOWN_CARD"
        logger.error(f"ICA: Game object is None. Player: {p_id_log}, Card: {card_name_log}.")
        return

    chat_id = game.get('chat_id') # Define chat_id for logging and further use
    if not chat_id:
        logger.error(f"ICA: chat_id missing from game_obj. Player: {player_id_who_discarded}, Game: {game}")
        return

    # Stale game object check
    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"ICA: Stale game_obj for C:{chat_id} passed to initiate_character_ability. Aborting.")
        return

    if game.get('active_ability_context'): # An ability is already in progress
        # ... (existing logic) ...
        return

    player_who_used_obj = game_state_manager.get_player_by_id(chat_id, player_id_who_discarded) # Uses validated chat_id
    if not player_who_used_obj:
        logger.error(f"ICA: Player {player_id_who_discarded} not found in chat {chat_id}.")
        # Attempt to advance turn to prevent stall if player object is missing
        # This assumes player_id_who_discarded was the current player
        game['current_player_id'] = player_id_who_discarded
        await advance_turn_or_continue_sequence(game, context)
        return

    ability_name = discarded_card.get('name')
    ability_card_template = next((c for c in CHARACTER_CARDS if c['name'] == ability_name), None)
    if not ability_card_template: logger.error(f"InitiateAbility: Card template for {ability_name} not found."); return # Should not happen
    
    ability_time = ability_card_template.get('ability_time', 20)
    job_suffix = f"{chat_id}_{player_id_who_discarded}_{ability_name.replace(' ', '')}_{int(time.time())}"
    timeout_job_name = f"ability_timeout_{job_suffix}"

    # This context is for the ability being initiated by player_id_who_discarded
    current_ability_context = {
        'player_id': player_id_who_discarded, 'card_name': ability_name,
        'ability_card_obj': discarded_card, 'step': 'initial_prompt', 
        'timeout_job_name': timeout_job_name, 'targets_chosen': [], 'cards_selected_indices': []
    }
    # Do NOT assign to game['active_ability_context'] yet if it's a human player, 
    # as the specific ability handler will set the phase and then assign.
    # For AI, assign it briefly if it's a simple direct execution.

    # Group notification about ability activation (unless it's reactive Killer which has own flow)
    is_reactive_killer_use = (ability_name == "The Killer" and game.get('active_ability_context') and \
                              game['active_ability_context'].get('card_name') == "The Killer" and \
                              game['active_ability_context'].get('step') == 'killer_select_killer_card') # This condition might need review

    if not is_reactive_killer_use :
         group_ability_announce_text = f"{get_player_mention(player_who_used_obj)} discarded <b>{escape_html(ability_name)}</b>"
         if ability_name not in ["The Witness", "The Alibi", "The Killer"]: # Passive or reactive
            group_ability_announce_text += f", activating its ability!"
            if ability_time > 0 and not player_who_used_obj.get('is_ai'): # Only mention time for human interactive abilities
                 group_ability_announce_text += f" ({ability_time}s for choices)"
         else: group_ability_announce_text += "."
         try: await context.bot.send_message(chat_id, group_ability_announce_text, parse_mode=ParseMode.HTML)
         except TelegramError as e: logger.error(f"Error sending group ability announcement: {e}")

    # --- AI Player Using Ability (Non-Interactive/Simplified) ---
    if player_who_used_obj.get('is_ai'):
        logger.info(f"AI {player_id_who_discarded} discarded {ability_name}. Processing AI's non-interactive use.")
        game['active_ability_context'] = current_ability_context # AI has context briefly for this execution

        ai_executed_something = False
        if ability_name == "The Lady":
            active_others = [p for p in game['players'] + game['ai_players'] if p['id'] != player_id_who_discarded and p.get('status') == PLAYER_STATES["ACTIVE"]]
            if active_others:
                human_targets = [p for p in active_others if not p.get('is_ai')]
                chosen_target = random.choice(human_targets) if human_targets else random.choice(active_others)
                logger.info(f"ICA: AI Lady ({player_id_who_discarded}) is targeting {get_player_mention(chosen_target)}.")

                game['active_ability_context']['targets_chosen'] = [chosen_target['id']]
                game['active_ability_context']['step'] = 'lady_confirm_target' # Set context for killer check
                original_context_signature = id(game['active_ability_context'])

                await context.bot.send_message(chat_id, f"🤖 {get_player_mention(player_who_used_obj)} (The Lady) is targeting {get_player_mention(chosen_target)}...", parse_mode=ParseMode.HTML)

                killer_initiated = await check_for_killer_reaction(
                    game['chat_id'], context, player_id_who_discarded, chosen_target['id'], "The Lady", original_context_signature
                )

                if not killer_initiated:
                    logger.info(f"ICA: AI Lady, Killer not initiated for target {chosen_target['id']}. Executing directly.")
                    await execute_the_lady_ability(game, context, player_id_who_discarded, chosen_target['id'])

                ai_executed_something = True        

        elif ability_name == "The Mole":
            if player_who_used_obj.get('hand'):
                card_idx_to_view = random.randrange(len(player_who_used_obj['hand']))
                player_who_used_obj.setdefault('viewed_card_indices', set()).add(card_idx_to_view)
                logger.info(f"ICA: AI Mole ({player_id_who_discarded}) 'peeks' at their own card #{card_idx_to_view+1}.")
                await context.bot.send_message(chat_id, f"🤫 {get_player_mention(player_who_used_obj)} (The Mole) discreetly checked one of their own cards.", parse_mode=ParseMode.HTML)
                game['active_ability_context'] = None
                await advance_turn_or_continue_sequence(game, context)
                ai_executed_something = True
            
        elif ability_name == "The Mamma":
            active_others = [p for p in game['players'] + game['ai_players'] if p['id'] != player_id_who_discarded and p.get('status') == PLAYER_STATES["ACTIVE"]]
            if active_others:
                chosen_target = random.choice(active_others)
                logger.info(f"ICA: AI Mamma ({player_id_who_discarded}) is targeting {get_player_mention(chosen_target)}.")
                
                # Set context for the Killer check
                game['active_ability_context']['targets_chosen'] = [chosen_target['id']]
                game['active_ability_context']['step'] = 'mamma_confirmed_target'
                original_context_signature = id(game['active_ability_context'])
                
                # Announce the AI's intended action
                await context.bot.send_message(chat_id, f"🤖 {get_player_mention(player_who_used_obj)} (Mamma) is targeting {get_player_mention(chosen_target)}...", parse_mode=ParseMode.HTML)

                # Use the proper Killer check flow
                killer_initiated = await check_for_killer_reaction(
                    game['chat_id'], context, player_id_who_discarded, chosen_target['id'], "The Mamma", original_context_signature
                )

                # If the target can't or doesn't use Killer, execute the ability
                if not killer_initiated:
                    logger.info(f"ICA: AI Mamma, Killer not initiated for target {chosen_target['id']}. Executing directly.")
                    await execute_the_mamma_ability(game, context, player_id_who_discarded, chosen_target['id'])
                
                ai_executed_something = True        

        elif ability_name == "Police Patrol":
            active_others = [p for p in game['players'] + game['ai_players'] if p['id'] != player_id_who_discarded and p.get('status') == PLAYER_STATES["ACTIVE"]]
            target_p = random.choice(active_others) if active_others else None

            if target_p and target_p.get('hand'):
                card_idx = random.randrange(len(target_p['hand']))
                logger.info(f"ICA: AI Police ({player_id_who_discarded}) is targeting Card #{card_idx+1} of {get_player_mention(target_p)}.")

                game['active_ability_context']['targets_chosen'] = [target_p['id']]
                game['active_ability_context']['cards_selected_indices'] = [card_idx]
                game['active_ability_context']['step'] = 'police_confirmed_block_target' # Set context for killer
                original_context_signature = id(game['active_ability_context'])

                await context.bot.send_message(chat_id, f"🤖 {get_player_mention(player_who_used_obj)} (Police Patrol) is targeting Card #{card_idx+1} of {get_player_mention(target_p)}...", parse_mode=ParseMode.HTML)
                
                killer_initiated = await check_for_killer_reaction(
                    game['chat_id'], context, player_id_who_discarded, target_p['id'], "Police Patrol", original_context_signature
                )
                
                if not killer_initiated:
                    logger.info(f"ICA: AI Police, Killer not initiated for target {target_p['id']}. Executing directly.")
                    await execute_police_patrol_ability(game, context, player_id_who_discarded, target_p['id'], card_idx)
                
                ai_executed_something = True       
       
        elif ability_name == "The Snitch":
            active_others = [p for p in game['players'] + game['ai_players'] if p['id'] != player_id_who_discarded and p.get('status') == PLAYER_STATES["ACTIVE"]]
            if active_others:
                num_targets = random.choice([1, 2]) if len(active_others) >= 2 else 1
                targets = random.sample(active_others, min(num_targets, len(active_others)))
                target_ids = [t['id'] for t in targets]
                target_mentions = ", ".join([get_player_mention(p) for p in targets])

                logger.info(f"ICA: AI Snitch ({player_id_who_discarded}) is targeting {target_mentions}.")
                
                game['active_ability_context']['targets_chosen'] = target_ids
                game['active_ability_context']['step'] = 'snitch_confirmed_targets'
                original_context_signature = id(game['active_ability_context'])
                
                await context.bot.send_message(chat_id, f"🤖 {get_player_mention(player_who_used_obj)} (Snitch) is targeting {target_mentions}...", parse_mode=ParseMode.HTML)

                killer_initiated = False
                # Killer can only be used if there is exactly one target
                if len(targets) == 1:
                    single_target_id = targets[0]['id']
                    killer_initiated = await check_for_killer_reaction(
                        game['chat_id'], context, player_id_who_discarded, single_target_id, "The Snitch", original_context_signature
                    )

                if not killer_initiated:
                    logger.info(f"ICA: AI Snitch, Killer not initiated for target(s) {target_ids}. Executing directly.")
                    await execute_the_snitch_ability(game, context, player_id_who_discarded, target_ids)

                ai_executed_something = True        

        elif ability_name in ["The Driver", "The Safecracker", "The Gangster"]:
             logger.info(f"ICA: AI {player_id_who_discarded} discarded complex ability {ability_name}. AI use is non-interactive/fizzles.")
        
        if not ai_executed_something: 
            logger.debug(f"ICA: AI ability {ability_name} for {player_id_who_discarded} fizzled or passive. Clearing context and advancing.")
            game['active_ability_context'] = None 
            await advance_turn_or_continue_sequence(game, context)
        
        return
    
    # --- Human Player's Ability Initiation (Interactive) ---
    game['active_ability_context'] = current_ability_context 

    if ability_name == "The Mole":
        game['active_ability_context']['step'] = 'mole_select_own_card'
        game['phase'] = GAME_PHASES["CHARACTER_ABILITY_ACTION"] 
        blocked_indices_mole = set(game.get('blocked_cards', {}).get(str(player_id_who_discarded), {}).keys())
        
        kbd = keyboards.get_card_selection_keyboard(
            purpose_prefix="ability_mole_do_peek", 
            player_hand=player_who_used_obj['hand'], 
            player_id_context=player_id_who_discarded,
            facedown=True, 
            num_to_select=1,
            min_to_select=1, # Must select exactly one
            currently_selected_indices=None, # No pre-selection
            allow_cancel=True, # Allow cancelling The Mole ability
            cancel_callback_data=f"ability_mole_overall_cancel_{player_id_who_discarded}",
            blocked_card_indices=blocked_indices_mole
        )
        await send_message_to_player(context, player_id_who_discarded, "The Mole! Choose one of your own cards (by position) to peek at:", reply_markup=kbd)
    elif ability_name == "The Lady":
        game['active_ability_context']['step'] = 'lady_select_target'
        game['phase'] = GAME_PHASES["CHARACTER_ABILITY_TARGETING"]
        kbd = keyboards.get_target_player_keyboard(game, player_id_who_discarded, "ability_lady_target", 1,1,None,None,f"ability_lady_overall_cancel_{player_id_who_discarded}")
        if not any(btn.text != "❌ Cancel Ability" for row in kbd.inline_keyboard for btn in row if isinstance(btn, InlineKeyboardButton)): # check if only cancel exists
            await send_message_to_player(context, player_id_who_discarded, "The Lady finds no one to target!"); game['active_ability_context'] = None; await advance_turn_or_continue_sequence(chat_id, context); return
        await send_message_to_player(context, player_id_who_discarded, "The Lady: Choose opponent to shuffle hand:", reply_markup=kbd)
    elif ability_name == "The Driver":
        game['active_ability_context']['step'] = 'driver_select_cards'
        game['phase'] = GAME_PHASES["CHARACTER_ABILITY_ACTION"]
        blocked_indices = set(game.get('blocked_cards', {}).get(player_id_who_discarded, {}).keys())
        kbd = keyboards.get_card_selection_keyboard("ability_driver_card", player_who_used_obj['hand'], player_id_who_discarded, True, 2, 1, None, True, f"ability_driver_overall_cancel_{player_id_who_discarded}", blocked_indices)
        await send_message_to_player(context, player_id_who_discarded, "The Driver! Discard 1 or 2 Bottle cards (facedown choice). Penalty for non-Bottles.", reply_markup=kbd)
    # Example for Safecracker (Human)
    elif ability_name == "The Safecracker":
        game['active_ability_context']['step'] = 'safecracker_initial_prompt'
        game['phase'] = GAME_PHASES["CHARACTER_ABILITY_ACTION"]
        kbd = keyboards.get_safe_interaction_keyboard(player_id_who_discarded)
        await send_message_to_player(context, player_id_who_discarded, "The Safecracker! View Safe & exchange a card?", reply_markup=kbd)
    # Example for Gangster (Human)
    elif ability_name == "The Gangster":
        game['active_ability_context']['step'] = 'gangster_select_action_type'
        game['active_ability_context']['swap_count'] = 0  # Initialize the swap counter
        game['phase'] = GAME_PHASES["CHARACTER_ABILITY_ACTION"]
        kbd = keyboards.get_gangster_action_type_keyboard(player_id_who_discarded)
        # Update the prompt to inform the user of two swaps
        await send_message_to_player(context, player_id_who_discarded, "The Gangster! You can perform up to two swaps. Choose your first action:", reply_markup=kbd)
    # Example for Police Patrol (Human)
    elif ability_name == "Police Patrol":
        game['active_ability_context']['step'] = 'police_select_target_player'
        game['phase'] = GAME_PHASES["CHARACTER_ABILITY_TARGETING"]
        kbd = keyboards.get_target_player_keyboard(game, player_id_who_discarded, "ability_police_player", 1,1,None,None,f"ability_police_overall_cancel_{player_id_who_discarded}")
        if not any(btn.text != "❌ Cancel Ability" for row in kbd.inline_keyboard for btn in row if isinstance(btn, InlineKeyboardButton)):
            await send_message_to_player(context, player_id_who_discarded, "Police Patrol finds no one to target!"); game['active_ability_context'] = None; await advance_turn_or_continue_sequence(chat_id, context); return
        await send_message_to_player(context, player_id_who_discarded, "Police Patrol: Choose opponent to target:", reply_markup=kbd)
    # Example for Snitch (Human)
    elif ability_name == "The Snitch":
        game['active_ability_context']['step'] = 'snitch_select_targets'
        game['phase'] = GAME_PHASES["CHARACTER_ABILITY_TARGETING"]
        kbd = keyboards.get_target_player_keyboard(game, player_id_who_discarded, "ability_snitch_target", 2,1,None,None,f"ability_snitch_overall_cancel_{player_id_who_discarded}")
        if not any(btn.text != "❌ Cancel Ability" for row in kbd.inline_keyboard for btn in row if isinstance(btn, InlineKeyboardButton)):
            await send_message_to_player(context, player_id_who_discarded, "The Snitch finds no one to target!"); game['active_ability_context'] = None; await advance_turn_or_continue_sequence(chat_id, context); return
        await send_message_to_player(context, player_id_who_discarded, "The Snitch: Choose 1 or 2 players:", reply_markup=kbd)
    # Example for Mamma (Human)
    elif ability_name == "The Mamma":
        game['active_ability_context']['step'] = 'mamma_select_target'
        game['phase'] = GAME_PHASES["CHARACTER_ABILITY_TARGETING"]
        kbd = keyboards.get_target_player_keyboard(game, player_id_who_discarded, "ability_mamma_target", 1,1,None,None,f"ability_mamma_overall_cancel_{player_id_who_discarded}")
        if not any(btn.text != "❌ Cancel Ability" for row in kbd.inline_keyboard for btn in row if isinstance(btn, InlineKeyboardButton)):
            await send_message_to_player(context, player_id_who_discarded, "Mamma finds no one to target!"); game['active_ability_context'] = None; await advance_turn_or_continue_sequence(chat_id, context); return
        await send_message_to_player(context, player_id_who_discarded, "Mamma: Choose player to target:", reply_markup=kbd)

    else:   
        logger.warning(f"InitiateAbility: Human discarded {ability_name} but no interactive setup defined. Fizzling.")
        game['active_ability_context'] = None
        await advance_turn_or_continue_sequence(chat_id, context)
        return  

    # Schedule timeout for human's interactive ability step
    if game.get('active_ability_context'): 
        context.job_queue.run_once(character_ability_timeout_job, ability_time,
                                   data={'chat_id': chat_id, 'expected_context_signature': id(game['active_ability_context'])},
                                   name=timeout_job_name)

async def process_discarded_card(game_obj: dict, context: ContextTypes.DEFAULT_TYPE,
                                 player_id_who_discarded: Union[int, str],
                                 discarded_card: dict):
    game = game_obj
    if not game:
        p_id_log = player_id_who_discarded if player_id_who_discarded else "UNKNOWN_PLAYER"
        card_name_log = discarded_card.get('name') if discarded_card and isinstance(discarded_card, dict) else "UNKNOWN_CARD"
        logger.error(f"PDC: Game object is None. Player: {p_id_log}, Card: {card_name_log}.")
        return

    chat_id = game.get('chat_id')
    if not chat_id:
        logger.error(f"PDC: chat_id missing from game_obj. Player: {player_id_who_discarded}, Card: {discarded_card.get('name') if discarded_card else 'N/A'}, Game: {game}")
        return

    # Stale game object check - Very important!
    current_game_in_manager = game_state_manager.get_game(chat_id)
    if not current_game_in_manager or id(current_game_in_manager) != id(game):
        logger.warning(f"PDC: Stale game_obj for C:{chat_id} (ID: {id(game)}) vs manager's game (ID: {id(current_game_in_manager) if current_game_in_manager else 'None'}). Aborting PDC.")
        return
    
    player_obj_who_discarded = game_state_manager.get_player_by_id(chat_id, player_id_who_discarded) 
    if not player_obj_who_discarded:
        logger.error(f"PDC: Player {player_id_who_discarded} not found in chat {chat_id} (using validated game).")
        game['current_player_id'] = player_id_who_discarded 
        await advance_turn_or_continue_sequence(game, context) 
        return

    logger.info(f"PDC: Processing discarded card '{discarded_card.get('name')}' by P:{player_id_who_discarded} (AI:{player_obj_who_discarded.get('is_ai', False)}) in C:{chat_id}.")

    if not game.get('discard_pile') or game['discard_pile'][-1] != discarded_card:
        logger.warning(f"PDC: Discarded card {discarded_card.get('name')} is not the top of the discard pile. Top is: {game['discard_pile'][-1].get('name') if game.get('discard_pile') else 'Empty'}. Processing effects for {discarded_card.get('name')}, assuming it's the intended card.")

    if discarded_card.get('type') == 'bottle':
        logger.debug(f"PDC: Discarded card is a Bottle: {discarded_card.get('name')}. Initiating bottle match window.")
        await initiate_bottle_matching_window(game, context, discarded_card) 
        return

    elif discarded_card.get('type') == 'character':
        ability_name = discarded_card.get('name')
        is_killer_card = (ability_name == "The Killer")
        is_killer_being_used_reactively = False
        active_ctx_for_killer_check = game.get('active_ability_context')
        if active_ctx_for_killer_check and \
           active_ctx_for_killer_check.get('card_name') == "The Killer" and \
           active_ctx_for_killer_check.get('player_id') == player_id_who_discarded and \
           active_ctx_for_killer_check.get('step') == 'killer_select_killer_card':
            is_killer_being_used_reactively = True
            
        if ability_name in ["The Witness", "The Alibi"] or (is_killer_card and not is_killer_being_used_reactively):
            logger.info(f"PDC: Passive/non-reactive character card '{ability_name}' discarded by {player_id_who_discarded}. No special interactive action from *this specific discard*. Advancing turn.")
            game['current_player_id'] = player_id_who_discarded
            await advance_turn_or_continue_sequence(game, context)
            return
        else: 
            logger.debug(f"PDC: Interactive character card '{ability_name}' discarded by {player_id_who_discarded}. Initiating ability. Reactive Killer: {is_killer_being_used_reactively}")
            await initiate_character_ability(game, context, player_id_who_discarded, discarded_card) 
            return 
    else:
        logger.warning(f"PDC: Unknown card type '{discarded_card.get('type')}' for card '{discarded_card.get('name')}' discarded by {player_id_who_discarded}. Advancing turn.")
        game['current_player_id'] = player_id_who_discarded 
        await advance_turn_or_continue_sequence(game, context) 
        return

    logger.error(f"PDC: Reached unexpected end of function for card {discarded_card.get('name')} by {player_id_who_discarded}. Type: {discard_card.get('type')}. This indicates a logic flaw. Advancing turn as fallback.")
    game['current_player_id'] = player_id_who_discarded
    await advance_turn_or_continue_sequence(game, context)

async def process_cards_deal_and_viewing_start(chat_id: int, context: ContextTypes.DEFAULT_TYPE): # CALLED BY AL CAPONE (HUMAN/AI)
    logger.info(f"process_cards_deal_and_viewing_start: ENTERED for chat {chat_id}")
    game = game_state_manager.get_game(chat_id)
    if not game: logger.error(f"process_cards_deal_and_viewing_start: Game object None for {chat_id}. Aborting."); return
    if game['phase'] == GAME_PHASES["VIEWING"]: logger.warning(f"process_cards_deal_and_viewing_start: Already in VIEWING for {chat_id}. Skipping."); return
    if game['phase'] != GAME_PHASES["DEALING_CARDS"]: game['phase'] = GAME_PHASES["DEALING_CARDS"]
    logger.info(f"process_cards_deal_and_viewing_start: Phase set to DEALING_CARDS for {chat_id}.")
    if not game_state_manager.deal_cards_to_players(chat_id):
        logger.error(f"process_cards_deal_and_viewing_start: Error during deal_cards_to_players for {chat_id}. Cannot continue.")
        try: await context.bot.send_message(chat_id, "Error dealing cards. Try /newgame.")
        except TelegramError as e: logger.error(f"process_cards_deal_and_viewing_start: Failed to send dealing error: {e}")
        game_state_manager.end_game(chat_id); return
    logger.info(f"process_cards_deal_and_viewing_start: Cards dealt for {chat_id}.")
    try: await context.bot.send_message(chat_id, f"🎴 Cards dealt! Each gets {HAND_CARDS_COUNT}. {SAFE_CARDS_COUNT} in Safe.", parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"process_cards_deal_and_viewing_start: Failed to send 'cards dealt' message: {e}")
    game['phase'] = GAME_PHASES["VIEWING"]; logger.info(f"process_cards_deal_and_viewing_start: Phase VIEWING for {chat_id}.")
    game['viewing_start_time'] = time.time()
    job_suffix = f"{chat_id}_{int(time.time())}"; viewing_timeout_job_name = f"viewing_timeout_{job_suffix}"
    game['viewing_timer_job_name'] = viewing_timeout_job_name
    human_players = [p for p in game.get('players', []) if not p.get('is_ai')]; ai_count = len(game.get('ai_players', []))
    logger.debug(f"process_cards_deal_and_viewing_start: Preparing PMs for {len(human_players)} humans in {chat_id}.")
    for p_data in human_players:
        p_data['viewed_card_indices'] = set(); p_data['viewed_all_initial_cards'] = False
        hand_len = len(p_data.get('hand', []))
        if hand_len == 0 and HAND_CARDS_COUNT > 0: 
            logger.warning(f"Player {p_data['id']} has no cards for viewing. Expected {HAND_CARDS_COUNT}.")
            await send_message_to_player(context, p_data['id'], "Error: No cards to view."); continue
        instr = (f"Your {hand_len} card(s). View any {INITIAL_CARDS_TO_VIEW} (if available).\n"
                 f"{CARD_VIEWING_TIME_LIMIT}s. Click to peek.")
        # IMPORTANT: If this PM button needs to find the group game, its callback_data needs group_chat_id
        # For now, assuming keyboards.get_card_viewing_keyboard creates callbacks that don't need explicit group_chat_id if used in PMs
        # (which implies user_data or other mechanism for context)
        msg_id = await send_message_to_player(context, p_data['id'], instr,
            reply_markup=keyboards.get_card_viewing_keyboard(p_data['hand'], p_data['viewed_card_indices'], INITIAL_CARDS_TO_VIEW, HAND_CARDS_COUNT))
        if msg_id: p_data['viewing_message_id'] = msg_id; logger.debug(f"Viewing PM to {p_data['id']}, msg_id: {msg_id}")
        else: logger.warning(f"Failed to send viewing PM to {p_data['id']}.")
    group_msg = (f"Players, check PMs to view {INITIAL_CARDS_TO_VIEW} cards (if enough). {CARD_VIEWING_TIME_LIMIT}s.")
    if ai_count > 0: group_msg += f" {ai_count} AI mobsters also 'peeking'."
    try: await context.bot.send_message(chat_id, group_msg, parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"process_cards_deal_and_viewing_start: Failed group viewing instructions: {e}")
    logger.info(f"process_cards_deal_and_viewing_start: Scheduling job '{viewing_timeout_job_name}' for {chat_id}.")
    context.job_queue.run_once(viewing_timeout_job, CARD_VIEWING_TIME_LIMIT,  # This is the function to call
                               data={'chat_id': chat_id, 'expected_viewing_timer_job_name': viewing_timeout_job_name}, 
                               name=viewing_timeout_job_name)

async def initiate_game_start_sequence(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"initiate_game_start_sequence: ENTERED for chat {chat_id}.") 
    game = game_state_manager.get_game(chat_id)
    if not game: logger.error(f"initiate_game_start_sequence: Game object None for {chat_id}. Aborting."); return
    game['phase'] = GAME_PHASES["GANGSTER_ASSIGNMENT"]
    logger.info(f"initiate_game_start_sequence: Phase GANGSTER_ASSIGNMENT for {chat_id}.")
    try: await context.bot.send_message(chat_id, "Finalizing players... Assigning gangsters! 🕴️", parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Failed to send 'assigning gangsters' msg: {e}")
    logger.info(f"initiate_game_start_sequence: Calling assign_gangsters_to_players for {chat_id}.")
    if not game_state_manager.assign_gangsters_to_players(chat_id) or not game.get('al_capone_player_id'):
        logger.error(f"Error assigning gangsters/Al Capone for {chat_id}. Cannot start.")
        try: await context.bot.send_message(chat_id, "Error assigning gangsters. Try /newgame.")
        except TelegramError as e: logger.error(f"Failed to send gangster assignment error: {e}")
        game_state_manager.end_game(chat_id); return
    logger.info(f"initiate_game_start_sequence: Gangsters assigned. AC ID: {game.get('al_capone_player_id')}.")
    all_participants_sorted = sorted(game.get('players', []) + game.get('ai_players', []), key=lambda p: p.get('join_time', 0))
    announcements = ["<b>Gangster Assignments (join order):</b>"]; ac_mention = "<i>Unknown force</i>"
    for p_data in all_participants_sorted:
        g_name = p_data.get('gangster', 'Mobster'); mention = get_player_mention(p_data)
        announcements.append(f"{mention} is <b>{escape_html(g_name)}</b>")
        if p_data['id'] == game.get('al_capone_player_id'): ac_mention = mention
        if not p_data.get('is_ai'):
            g_info = GANGSTER_INFO.get(g_name, {}); caption = f"You are <b>{escape_html(g_name)}</b>!\n<i>{escape_html(g_info.get('nickname','A figure of intrigue...'))}</i>\n\n{g_info.get('info','A notorious figure.')}"
            img = g_info.get('image', DEFAULT_GANGSTER_IMAGE)
            try: await context.bot.send_photo(chat_id=p_data['id'], photo=img, caption=caption, parse_mode=ParseMode.HTML)
            except TelegramError as e: 
                logger.warning(f"Failed gangster PM photo to {p_data['id']} ({g_name}): {e}. Sending text.")
                await send_message_to_player(context, p_data['id'], caption)
    try: await context.bot.send_message(chat_id, "\n".join(announcements), parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Failed group gangster announcement: {e}")
    game['phase'] = GAME_PHASES["WAIT_FOR_AL_CAPONE_CONTINUE"]
    logger.info(f"initiate_game_start_sequence: Phase WAIT_FOR_AL_CAPONE_CONTINUE for {chat_id}.")
    ac_player = game_state_manager.get_player_by_id(chat_id, game.get('al_capone_player_id'))
    if ac_player:
        if not ac_player.get('is_ai'):
            logger.info(f"AC {ac_player['id']} is human. Sending 'Continue' PM.")
            msg_id = await send_message_to_player(context, ac_player['id'], "You are Al Capone! Start the game by pressing below.", reply_markup=keyboards.get_al_capone_continue_keyboard())
            if msg_id: game['al_capone_continue_message_id'] = msg_id
            try: await context.bot.send_message(chat_id, f"{ac_mention}, as Al Capone, must continue the game...", parse_mode=ParseMode.HTML)
            except TelegramError as e: logger.error(f"Failed group wait for AC msg: {e}")
        else: # AI Al Capone
            logger.info(f"AC {ac_player['id']} is AI. Proceeding auto.")
            try: await context.bot.send_message(chat_id, f"{ac_mention} (AI Al Capone) starts the game... Dealing cards!", parse_mode=ParseMode.HTML)
            except TelegramError as e: logger.error(f"Failed AI AC group msg: {e}")
            await process_cards_deal_and_viewing_start(chat_id, context) # AI "presses" continue
    else: 
        logger.critical(f"CRITICAL: Al Capone player object NOT FOUND for ID {game.get('al_capone_player_id')} in {chat_id}. Ending.")
        try: await context.bot.send_message(chat_id, "Critical error: Al Capone config failed. Try /newgame.")
        except TelegramError as e: logger.error(f"Failed critical AC error msg: {e}")
        game_state_manager.end_game(chat_id); return

# --- Command Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat_id = update.effective_chat.id
    display_name = escape_html(user.first_name)
    
    # Define the paddings and the welcome message
    padding_chicago = "          "
    padding_omerta = "                      "
    welcome_message = (f"{padding_chicago}🕵️ <b>CHICAGO, 1932</b>\n\n"
                       f"The streets drip with bootleg booze and betrayal. You're no petty crook. You're a <b>mob boss!</b>\n\n"
                       f"But the feds are closing in and your rivals are ready to sell you out. Your stash? Hotter by the minute.\n\n"
                       f"Trade fast. Bluff hard before someone calls...\n\n"
                       f"{padding_omerta}❗️❗️<b>OMERTA</b>❗️❗️\n\n"
                       f"By then it's game over. Hands up. Bottles down.\n\n"
                       f"Whoever holds the most heat takes the fall.\nThe one with the least? Walks free.\n\n"
                       f"Only ONE escapes the spotlight and rules the underworld.\n\n"
                       f"Could you be <b>HIM</b>?\n\n"
                       f"🎭 <b>OMERTA: DON'T GET CAUGHT</b> 🎭\n\n"
                       f"Trust no one. Swap fast. Think faster.\n\n"
                       f"Hi {display_name}! Use the buttons below to start a new game or get help.")

    # Get both the inline (buttons under message) and reply (persistent keyboard) markups
    inline_reply_markup = keyboards.get_start_keyboard()
    reply_keyboard_markup = keyboards.get_main_reply_keyboard()
    
    photo_url = DEFAULT_GANGSTER_IMAGE
    
    # Send the main welcome message WITH the photo, caption, and persistent keyboard
    await context.bot.send_photo(
        chat_id=chat_id,
        photo=photo_url,
        caption=welcome_message,
        reply_markup=reply_keyboard_markup,  # Attach the persistent keyboard here
        parse_mode=ParseMode.HTML
    )
    
    # Send a separate, clean message with the inline buttons for starting a game
    await context.bot.send_message(
        chat_id=chat_id,
        text="Choose an option to begin:",
        reply_markup=inline_reply_markup  # Attach the inline buttons here
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_text = ( "<b>Omerta: Don't Get Caught - Help</b>\n\n"
                  "<b>Objective:</b> Have the lowest point total when 'Omerta' is called.\n\n"
                  "<b>Key Commands & Actions:</b>\n"
                  "- /start: Initialize the bot.\n"
                  "- /newgame: Start a new game session.\n"
                  "- /rules: Detailed game rules.\n"
                  "- /endgame: Force-ends the current game in this chat.\n\n"
                  "<i>Gameplay involves drawing cards, using character abilities, and strategically calling Omerta. Full rules via /rules.</i>")
    target_message = update.message if update.message else (update.callback_query.message if update.callback_query else None)
    try:
        if target_message : await target_message.reply_text(help_text, parse_mode=ParseMode.HTML)
        else: await context.bot.send_message(update.effective_chat.id, help_text, parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Error sending help text: {e}")

async def rules_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rules_text = (
        "<b>Welcome to Omerta! 🕵️‍♂️</b>\n\n"
        "The goal is simple: have the LOWEST score when someone ends the game by shouting 'OMERTA!'\n\n"
        "--- <b>Getting Started</b> ---\n"
        "<b>1. Get Your Cards:</b> You start with 4 secret cards in your hand.\n"
        "<b>2. Take a Peek:</b> At the very beginning, you get to look at just TWO of your secret cards. Try to remember what they are and where they are!\n\n"
        "--- <b>What To Do On Your Turn</b> ---\n"
        "You can do ONE of these things:\n"
        "<b>1. Draw a Card 🃏:</b> Take a new card from the deck and swap it with any one of your secret cards.\n"
        "<b>2. Take from the Pile ♻️:</b> If the top card of the discard pile is a Bottle or an Alibi, you can take it and swap it with one of your cards.\n"
        "<b>3. Call OMERTA! 🗣️:</b> If you think your score is low enough, you can end the round! (See below)\n\n"
        "--- <b>Special Cards & Powers</b> ---\n"
        "When you discard a card, something might happen!\n\n"
        "🍾 <b>Bottle Cards:</b> When anyone discards a Bottle, it's a race! If you have a bottle with the same number, you have 5 seconds to try and discard yours too!\n\n"
        "🕴️ <b>Character Cards:</b> Discarding these lets you use a special power!\n"
        " • <b>The Lady 💃:</b> Mixes up an opponent's secret cards.\n"
        " • <b>The Mole 🤫:</b> Lets you peek at one of your OWN secret cards.\n"
        " • <b>The Gangster 🤝:</b> Swap a card with anyone.\n"
        " • <b>The Driver 🚗:</b> Lets you discard up to two of your Bottle cards.\n"
        " • <b>The Snitch 🗣:</b> Gives a random card from the deck to other players.\n"
        " • <b>The Safecracker 💰:</b> Swap one of your cards with a mystery card in the Safe.\n"
        " • <b>The Mamma 👵:</b> Makes another player skip their next turn.\n"
        " • <b>Police Patrol 🚨:</b> Freezes one of an opponent's cards for two rounds.\n"
        " • <b>The Killer 💥:</b> The ultimate defense! Use this to CANCEL an ability that someone is using on you.\n"
        " • <b>The Witness & The Alibi:</b> These characters have no powers, they're just worth points.\n\n"
        "--- <b>Ending the Game: The OMERTA Call</b> ---\n"
        "When someone calls Omerta, everyone shows their cards!\n\n"
        "🎉 To win, the person who called it must have the **lowest score**, and that score must be **7 or less**.\n"
        "😬 If they are wrong, they get a big +20 point penalty, and whoever *actually* had the lowest score wins the round instead!\n\n"
        "Good luck, boss!"
    )
    target_message = update.message if update.message else (update.callback_query.message if update.callback_query else None)
    try:
        if target_message : await target_message.reply_text(rules_text, parse_mode=ParseMode.HTML)
        else: await context.bot.send_message(update.effective_chat.id, rules_text, parse_mode=ParseMode.HTML)
    except TelegramError as e: logger.error(f"Error sending rules text: {e}")

async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays leaderboard options."""
    await update.message.reply_text(
        "Select a leaderboard to view:",
        reply_markup=keyboards.get_leaderboard_options_keyboard()
    )

async def new_game_command_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user = update.effective_user
    existing_game = game_state_manager.get_game(chat_id)
    if existing_game and existing_game['phase'] != GAME_PHASES["COMPLETED"]:
        await update.message.reply_text("A game is already in progress. Use 🛑 End Game first.")
        return
    
    game_state_manager.add_game(chat_id, user.id, user.first_name, user.username)
    reply_markup = keyboards.get_game_mode_keyboard()
    
    # Send a photo with the caption instead of just text
    await update.message.reply_photo(
        photo=SETUP_IMAGE_URL,
        caption="<b>Alright, boss. How do you want to run this operation?</b>",
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )

async def endgame_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id; game = game_state_manager.get_game(chat_id)
    if not game: await update.message.reply_text("No active game to end."); return
    
    logger.info(f"User {update.effective_user.id} initiated /endgame for chat {chat_id}.")
    job_keys_in_game_state = ['join_end_job_name', 'join_reminder_job_name', 'viewing_timer_job_name']
    for key in job_keys_in_game_state:
        if game.get(key): cancel_job(context, game.get(key))
    
    active_ability_ctx = game.get('active_ability_context')
    if active_ability_ctx and active_ability_ctx.get('timeout_job_name'):
        cancel_job(context, active_ability_ctx['timeout_job_name'])
    
    bottle_match_ctx = game.get('bottle_match_context')
    if bottle_match_ctx and bottle_match_ctx.get('timeout_job_name'):
        cancel_job(context, bottle_match_ctx['timeout_job_name'])
    
    # Cancel any dynamically named AI bottle match jobs (this is harder without storing all their names)
    # A better approach is for AI jobs to check game phase and self-terminate if game ended.
    # For now, rely on game state change to stop AI jobs from acting.

    game_state_manager.end_game(chat_id)
    await update.message.reply_text("The game has been manually ended. Use /newgame to start another!")
    logger.info(f"Game in chat {chat_id} ended by /endgame command from user {update.effective_user.id}.")

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    data = query.data
    
    game_chat_id_for_logic = update.effective_chat.id 

    try: await query.answer()
    except TelegramError as e: logger.warning(f"CBQ Answer Fail for {data} by U:{user.id} in C:{update.effective_chat.id}: {e}"); return

    game = game_state_manager.get_game(game_chat_id_for_logic)
    logger.debug(f"CBQ: {data} | U:{user.id} | C:{update.effective_chat.id} | GameLogicC:{game_chat_id_for_logic} | GamePhase: {game.get('phase') if game else 'No Game'}")

    if not game and not data.startswith("main_") and not data.startswith("play_again_"): # Allow play_again even if game cleared
        try:
            error_text = "Game not found or button is for an old session. Start /newgame?"
            # Simplified error sending
            await context.bot.send_message(update.effective_chat.id, error_text)
            if query.message and hasattr(query.message, 'edit_reply_markup'):
                 await query.edit_message_reply_markup(reply_markup=None)
        except TelegramError as te: logger.warning(f"Error sending 'no game' message / clearing markup: {te}")
        return
    
    active_ability_ctx = game.get('active_ability_context') if game else None

    # === Main Menu & Setup Callbacks ===
    if data == "main_new_game" or data == "play_again_new_game":
        current_button_chat_id = update.effective_chat.id
        game_chat_id_for_logic = current_button_chat_id 
        
        existing_game_in_current_chat = game_state_manager.get_game(current_button_chat_id)
        if existing_game_in_current_chat and existing_game_in_current_chat['phase'] != GAME_PHASES["COMPLETED"]:
            if data == "play_again_new_game": 
                await query.answer("Previous game instance still active. Try /endgame if stuck.", show_alert=True)
                return
        
        game = game_state_manager.add_game(current_button_chat_id, user.id, user.first_name, user.username)
        reply_markup = keyboards.get_game_mode_keyboard()
        new_caption = "<b>Alright, boss. How do you want to run this operation?</b>"        
        
        try:
            await query.edit_message_media(
                media=InputMediaPhoto(media=SETUP_IMAGE_URL, caption=new_caption, parse_mode=ParseMode.HTML),
                reply_markup=reply_markup
            )
        except TelegramError as e:
            logger.error(f"Error editing message media for new game: {e}")
            # Fallback if editing fails
            await context.bot.send_photo(
                chat_id=current_button_chat_id, photo=SETUP_IMAGE_URL, caption=new_caption,
                reply_markup=reply_markup, parse_mode=ParseMode.HTML
            )
        return 

    elif data == "main_help":
        help_text = ("<b>Omerta: Don't Get Caught - Help</b>\n\n" 
                     "<b>Objective:</b> Have the lowest point total when 'Omerta' is called.\n\n"
                     "Use /rules for detailed game mechanics and character ability descriptions.\n"
                     "Use /endgame to stop a current game.\n"
                     "Use /newgame to start a fresh one if no game is active.")
        try:
            await context.bot.send_message(update.effective_chat.id, help_text, parse_mode=ParseMode.HTML)
            if query.message.photo and hasattr(query.message, 'edit_reply_markup'): 
                await query.edit_message_reply_markup(reply_markup=None)
        except TelegramError as e: 
            logger.error(f"Error sending for main_help: {e}")
        except Exception as e_global:
            logger.critical(f"main_help: CRITICAL UNHANDLED ERROR: {e_global}", exc_info=True)
        return

    elif data == "mode_select_solo":
        if not game or game['phase'] != GAME_PHASES["SETUP"]: 
            await query.answer("Cannot select mode now. Game not in setup phase.", show_alert=True); return
        game['mode'] = GAME_MODES["SOLO"]; game['phase'] = GAME_PHASES["JOINING"] 
        logger.info(f"mode_select_solo: Chat {game_chat_id_for_logic} mode set to SOLO, phase to JOINING (will become SETUP).")
        
        game['phase'] = GAME_PHASES["SETUP"] 

        host_player = game_state_manager.add_player_to_game(game_chat_id_for_logic, user.id, user.first_name, user.username)
        if not host_player:
            logger.error(f"mode_select_solo: Failed to add host player {user.id}. Ending."); 
            await context.bot.send_message(game_chat_id_for_logic, "Error: Could not add you. Try /newgame.")
            game_state_manager.end_game(game_chat_id_for_logic); return
        
        logger.info(f"mode_select_solo: Host player {user.id} added.")
        game_state_manager.add_ai_player_to_game(game_chat_id_for_logic, "AI") 
        game_state_manager.add_ai_player_to_game(game_chat_id_for_logic, "AI")
        
        num_players = len(game.get('players', [])) + len(game.get('ai_players', []))
        player_list_str = format_player_list_html(game)
        user_mention = get_player_mention(host_player)
        
        txt = (f"<b>Solo Mode selected!</b> You ({user_mention}) and {len(game.get('ai_players',[]))} AI are in.\n\n"
               f"<b>Players ({num_players}/{MAX_PLAYERS}):</b>\n{player_list_str}\n\n")
        if num_players < MIN_PLAYERS:
            txt += f"You need at least {MIN_PLAYERS} total players. Add {MIN_PLAYERS - num_players} more AI to start."
        else:
            txt += f"You can add more AI (up to {MAX_PLAYERS - num_players} more) or, if ready, start the game."
            
        kbd = keyboards.get_setup_phase_keyboard(num_players >= MIN_PLAYERS, True, num_players, MAX_PLAYERS, MIN_PLAYERS)
        try:
            # Edit the caption of the photo message
            await query.edit_message_caption(caption=txt, reply_markup=kbd, parse_mode=ParseMode.HTML)
        except TelegramError as e: 
            logger.error(f"Err edit mode_solo caption: {e}")
        return

    elif data == "mode_select_group":
        if not game or game['phase'] != GAME_PHASES["SETUP"]:
            await query.answer("Cannot select mode now.", show_alert=True)
            return

        game['mode'] = GAME_MODES["GROUP"]
        game['phase'] = GAME_PHASES["JOINING"]
        host_player = game_state_manager.add_player_to_game(game_chat_id_for_logic, user.id, user.first_name, user.username)
        
        if not host_player:
            logger.error(f"mode_select_group: Failed to add host {user.id}. Ending.")
            await context.bot.send_message(game_chat_id_for_logic, "Error: Could not add you. Try /newgame.")
            game_state_manager.end_game(game_chat_id_for_logic)
            return
        
        game['join_start_time'] = time.time()
        job_suffix = f"{game_chat_id_for_logic}_{int(time.time())}"
        game['join_end_job_name'] = f"join_end_{job_suffix}"
        game['join_reminder_job_name'] = f"join_reminder_{job_suffix}"
        
        context.job_queue.run_once(join_period_ended_job, JOIN_TIME_LIMIT, data={'chat_id': game_chat_id_for_logic, 'expected_job_name': game['join_end_job_name']}, name=game['join_end_job_name'])
        context.job_queue.run_repeating(send_join_reminder_job, JOIN_REMINDER_INTERVAL, first=JOIN_REMINDER_INTERVAL, data={'chat_id': game_chat_id_for_logic}, name=game['join_reminder_job_name'])
        
        # Edit the photo message the user interacted with
        try:
            await query.edit_message_caption(
                caption=f"Group mode initiated by {get_player_mention(host_player)}. A join message will be sent below.",
                reply_markup=None,  # Remove the keyboard from this message
                parse_mode=ParseMode.HTML
            )
        except TelegramError as e:
            logger.error(f"Error editing caption for group mode selection: {e}")
        
        # Send the separate, new message for joining the game
        num_players = len(game.get('players', [])) + len(game.get('ai_players', []))
        join_msg_txt = (f"👥 A showdown is brewing, started by {get_player_mention(host_player)}! Press the button to join the action.\n"
                        f"Lobby open for approx. {JOIN_TIME_LIMIT} seconds.\n\n"
                        f"<b>Players ({num_players}/{MAX_PLAYERS}):</b>\n{format_player_list_html(game)}")
        try:
            join_keyboard = keyboards.get_join_game_keyboard(game, MAX_PLAYERS)
            join_msg = await context.bot.send_message(game_chat_id_for_logic, join_msg_txt, reply_markup=join_keyboard, parse_mode=ParseMode.HTML)
            if join_msg:
                game['join_message_id'] = join_msg.message_id
        except TelegramError as e:
            logger.error(f"Failed to send join message for group game: {e}")
            
        return

    elif data == "lobby_join_game":
        if not game or game['phase'] != GAME_PHASES["JOINING"]: await query.answer("Joining period is over or game hasn't started setup.", show_alert=True); return
        num_total_players = len(game.get('players', [])) + len(game.get('ai_players', []))
        if num_total_players >= MAX_PLAYERS: await query.answer("The game is already full!", show_alert=True); return
        if any(p['id'] == user.id for p in game.get('players',[])): await query.answer("You've already joined!", show_alert=True); return
        
        added_player = game_state_manager.add_player_to_game(game_chat_id_for_logic, user.id, user.first_name, user.username)
        if added_player:
            await query.answer("You've joined the game!", show_alert=False)
            await _update_join_message(context, game_chat_id_for_logic, game) 
            
            num_total_players = len(game.get('players', [])) + len(game.get('ai_players', [])) 
            if num_total_players >= MAX_PLAYERS: 
                cancel_job(context, game.get('join_end_job_name'))
                cancel_job(context, game.get('join_reminder_job_name'))
                context.job_queue.run_once(lambda ctx: asyncio.create_task(process_join_period_end(game_chat_id_for_logic, ctx)), 0.5, name=f"force_join_end_{game_chat_id_for_logic}")
        else: await query.answer("Could not join. Game might be full or an error occurred.", show_alert=True)
        return
    
    elif data == "group_lobby_ask_add_ai":
        if not game or game['phase'] != GAME_PHASES["JOINING"]:
            await query.answer("Cannot add AI now.", show_alert=True); return
        if user.id != game.get('host_id'):
            await query.answer("Only the game host can add AI players.", show_alert=True); return

        num_current_players = len(game.get('players', [])) + len(game.get('ai_players', []))
        if num_current_players >= MAX_PLAYERS:
            await query.answer("Game is full! Cannot add AI.", show_alert=True); return

        game['temp_context_for_message_edit'] = {
            'original_message_id': game['join_message_id'],
            'purpose': 'group_ai_add_from_lobby' 
        }
        logger.info(f"Host {user.id} trying to add AI from group lobby. Temp context set for join_message_id: {game['join_message_id']}")

        text_to_edit = "<b>How many AI players would you like to add to the mayhem?</b>"
        kbd_to_show = keyboards.get_add_ai_options_keyboard(num_current_players, MAX_PLAYERS)
        try:
            await context.bot.edit_message_text(
                chat_id=game_chat_id_for_logic,
                message_id=game['join_message_id'],
                text=text_to_edit,
                reply_markup=kbd_to_show,
                parse_mode=ParseMode.HTML
            )
        except TelegramError as e:
            logger.error(f"group_lobby_ask_add_ai: Error editing join message: {e}")
            game.pop('temp_context_for_message_edit', None) 
        return

    elif data == "setup_ask_add_ai": 
        if not game or game['phase'] not in [GAME_PHASES["SETUP"]]: 
             await query.answer("Cannot add AI now (not in solo setup).",True); return
        
        game['temp_context_for_message_edit'] = {
            'original_message_id': query.message.message_id, 
            'purpose': 'solo_ai_add_from_setup' 
        }
        logger.info(f"Player {user.id} trying to add AI from solo setup. Temp context set for query.message.message_id: {query.message.message_id}")

        num_current_players = len(game.get('players', [])) + len(game.get('ai_players', []))
        if num_current_players >= MAX_PLAYERS: await query.answer("Game is full! Cannot add AI.", show_alert=True); game.pop('temp_context_for_message_edit', None); return
        
        try:
            target_message = query.message 
            text_to_edit = "How many AI players would you like to add to the mayhem?"
            kbd_to_show = keyboards.get_add_ai_options_keyboard(num_current_players, MAX_PLAYERS)
            if target_message.photo: await query.edit_message_caption(caption=text_to_edit, reply_markup=kbd_to_show, parse_mode=ParseMode.HTML)
            elif target_message.text: await query.edit_message_text(text=text_to_edit, reply_markup=kbd_to_show, parse_mode=ParseMode.HTML)
        except TelegramError as e: 
            logger.error(f"setup_ask_add_ai: Error editing message: {e}")
            game.pop('temp_context_for_message_edit', None)
        return

    elif data.startswith("ai_add_count_"):
        temp_ctx = game.get('temp_context_for_message_edit', {})
        original_msg_id_to_edit = temp_ctx.get('original_message_id')
        purpose = temp_ctx.get('purpose')

        if not original_msg_id_to_edit or not purpose:
            if game and game['phase'] == GAME_PHASES["SETUP"]: 
                 logger.warning("ai_add_count_: temp_context_for_message_edit missing, falling back to query.message for SOLO setup.")
                 original_msg_id_to_edit = query.message.message_id 
                 purpose = 'solo_ai_add_from_setup'
            else:
                await query.answer("Cannot add AI: context error or invalid phase.", show_alert=True); return
        
        if purpose == 'group_ai_add_from_lobby' and (not game or game['phase'] != GAME_PHASES["JOINING"]):
            await query.answer("Cannot add AI at this stage (group lobby).", show_alert=True); game.pop('temp_context_for_message_edit', None); return
        if purpose == 'solo_ai_add_from_setup' and (not game or game['phase'] != GAME_PHASES["SETUP"]):
            await query.answer("Cannot add AI at this stage (solo setup).", show_alert=True); game.pop('temp_context_for_message_edit', None); return

        try: count = int(data.split("_")[-1])
        except ValueError: logger.error(f"ai_add_count_: Bad count from {data}"); await query.answer("Invalid AI count.",True); game.pop('temp_context_for_message_edit', None); return

        added_ai_count = 0
        for _ in range(count):
            if game_state_manager.add_ai_player_to_game(game_chat_id_for_logic, "AI"): 
                added_ai_count += 1
        
        game.pop('temp_context_for_message_edit', None) 

        if purpose == 'group_ai_add_from_lobby':
            await _update_join_message(context, game_chat_id_for_logic, game)
            if added_ai_count > 0:
                await context.bot.send_message(game_chat_id_for_logic, f"{get_player_mention(game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id))} (Host) added {added_ai_count} AI player(s).", parse_mode=ParseMode.HTML)
            num_total_players = len(game.get('players', [])) + len(game.get('ai_players', []))
            if num_total_players >= MAX_PLAYERS:
                cancel_job(context, game.get('join_end_job_name'))
                cancel_job(context, game.get('join_reminder_job_name'))
                context.job_queue.run_once(lambda c: asyncio.create_task(process_join_period_end(game_chat_id_for_logic, c)), 0.5, name=f"force_join_end_ai_{game_chat_id_for_logic}")

        elif purpose == 'solo_ai_add_from_setup':
            num_players = len(game.get('players', [])) + len(game.get('ai_players', []))
            player_list_str = format_player_list_html(game)
            is_solo = game['mode'] == GAME_MODES["SOLO"] 
            
            txt = f"{added_ai_count} AI player(s) added.\n\n<b>Players ({num_players}/{MAX_PLAYERS}):</b>\n{player_list_str}\n\n"
            if num_players < MIN_PLAYERS: txt += f"Need {MIN_PLAYERS - num_players} more player(s) to start."
            elif num_players < MAX_PLAYERS: txt += "You can add more AI or start the game."
            else: txt += "Maximum players reached. Ready to start!"

            kbd = keyboards.get_setup_phase_keyboard(num_players >= MIN_PLAYERS, is_solo, num_players, MAX_PLAYERS, MIN_PLAYERS)
            try: 
                target_message = query.message 
                if target_message.message_id == original_msg_id_to_edit: 
                    if target_message.photo : await query.edit_message_caption(caption=txt, reply_markup=kbd, parse_mode=ParseMode.HTML)
                    elif target_message.text : await query.edit_message_text(text=txt, reply_markup=kbd, parse_mode=ParseMode.HTML)
                else: 
                    logger.warning("ai_add_count_ (solo): Mismatch message ID for edit. Sending new.")
                    await context.bot.send_message(game_chat_id_for_logic, txt, reply_markup=kbd, parse_mode=ParseMode.HTML)

            except TelegramError as e: 
                logger.error(f"Error editing message after AI count add (solo): {e}")
                await context.bot.send_message(game_chat_id_for_logic, txt, reply_markup=kbd, parse_mode=ParseMode.HTML)
            
        return

    elif data == "ai_add_cancel":
        temp_ctx = game.get('temp_context_for_message_edit', {})
        original_msg_id_to_edit = temp_ctx.get('original_message_id')
        purpose = temp_ctx.get('purpose')

        if not original_msg_id_to_edit or not purpose:
            if game and game['phase'] == GAME_PHASES["SETUP"]: # Likely solo setup
                 logger.warning("ai_add_cancel: temp_context_for_message_edit missing, falling back to query.message for SOLO setup.")
                 original_msg_id_to_edit = query.message.message_id 
                 purpose = 'solo_ai_add_from_setup'
            else:
                await query.answer("Cannot cancel AI addition: context error.", show_alert=True); return

        # Basic phase check
        if purpose == 'group_ai_add_from_lobby' and (not game or game['phase'] != GAME_PHASES["JOINING"]):
            await query.answer("Cannot cancel AI add at this stage (group lobby).", show_alert=True); game.pop('temp_context_for_message_edit', None); return
        if purpose == 'solo_ai_add_from_setup' and (not game or game['phase'] != GAME_PHASES["SETUP"]):
            await query.answer("Cannot cancel AI add at this stage (solo setup).", show_alert=True); game.pop('temp_context_for_message_edit', None); return

        game.pop('temp_context_for_message_edit', None) # Clear temp context

        if purpose == 'group_ai_add_from_lobby':
            await _update_join_message(context, game_chat_id_for_logic, game) # Restore join message
            await context.bot.send_message(game_chat_id_for_logic, f"{get_player_mention(game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id))} (Host) cancelled AI addition.", parse_mode=ParseMode.HTML)

        elif purpose == 'solo_ai_add_from_setup':
            num_players = len(game.get('players', [])) + len(game.get('ai_players', []))
            player_list_str = format_player_list_html(game)
            is_solo = game['mode'] == GAME_MODES["SOLO"]
            txt = (f"AI addition cancelled.\n\n<b>Players ({num_players}/{MAX_PLAYERS}):</b>\n{player_list_str}\n\nChoose an option:")
            kbd = keyboards.get_setup_phase_keyboard(num_players >= MIN_PLAYERS, is_solo, num_players, MAX_PLAYERS, MIN_PLAYERS)
            try:
                target_message = query.message # The message that had AI count options
                if target_message.message_id == original_msg_id_to_edit:
                    if target_message.photo: await query.edit_message_caption(caption=txt, reply_markup=kbd, parse_mode=ParseMode.HTML)
                    elif target_message.text: await query.edit_message_text(text=txt, reply_markup=kbd, parse_mode=ParseMode.HTML)
                else:
                    logger.warning("ai_add_cancel (solo): Mismatch message ID for edit. Sending new.")
                    await context.bot.send_message(game_chat_id_for_logic, txt, reply_markup=kbd, parse_mode=ParseMode.HTML)
            except TelegramError as e: 
                logger.error(f"Error editing message for ai_add_cancel (solo): {e}")
                await context.bot.send_message(game_chat_id_for_logic, txt, reply_markup=kbd, parse_mode=ParseMode.HTML)
        return

    elif data == "setup_start_game":
        logger.debug(f"setup_start_game: Entered. Game phase: {game.get('phase') if game else 'No Game'}")
        if not game or game['phase'] not in [GAME_PHASES["JOINING"], GAME_PHASES["SETUP"]]: await query.answer("Not in state to be started.",True); return
        total_players = len(game.get('players', [])) + len(game.get('ai_players', []))
        logger.info(f"setup_start_game: Total players = {total_players}, MIN_PLAYERS = {MIN_PLAYERS}")
        if total_players < MIN_PLAYERS: await query.answer(f"Need at least {MIN_PLAYERS}, have {total_players}.",True); return
        if game['phase'] == GAME_PHASES["JOINING"]:
            logger.info(f"setup_start_game: Cancelling join jobs for group game.")
            cancel_job(context, game.get('join_end_job_name')); cancel_job(context, game.get('join_reminder_job_name'))
            if game.get('join_message_id'): 
                try: await context.bot.edit_message_reply_markup(game_chat_id_for_logic, game['join_message_id'], reply_markup=None)
                except TelegramError as e: logger.warning(f"setup_start_game: Failed to clear join message buttons: {e}")
        logger.info(f"setup_start_game: Conditions met. Editing query message and starting sequence.")
        try: 
            msg_to_edit = query.message
            edit_text = "Initializing game sequence... Get ready!"
            if msg_to_edit.photo : await query.edit_message_caption(caption=edit_text, reply_markup=None, parse_mode=ParseMode.HTML)
            elif msg_to_edit.text : await query.edit_message_text(edit_text, reply_markup=None, parse_mode=ParseMode.HTML)
            else: await context.bot.send_message(game_chat_id_for_logic, edit_text, parse_mode=ParseMode.HTML) # Fallback
            logger.debug(f"setup_start_game: Query message edited/sent successfully.")
        except TelegramError as e: logger.error(f"setup_start_game: Error editing message: {e}. Proceeding anyway.")
        except Exception as e_g: logger.critical(f"setup_start_game: CRITICAL ERROR before initiate: {e_g}", exc_info=True); return
        logger.info(f"setup_start_game: Calling initiate_game_start_sequence for chat {game_chat_id_for_logic}.")
        await initiate_game_start_sequence(game_chat_id_for_logic, context)
        return

    elif data == "setup_force_solo":
        if not game or game['phase'] != GAME_PHASES["JOINING"]: await query.answer("Cannot switch now.",True); return
        logger.info(f"setup_force_solo: Switching to Solo mode for chat {game_chat_id_for_logic}.")
        game['mode'] = GAME_MODES["SOLO"]
        current_players_count = len(game.get('players', [])) + len(game.get('ai_players', []))
        ai_needed = MIN_PLAYERS - current_players_count
        if len(game.get('players',[])) == 1 and current_players_count < MIN_PLAYERS and ai_needed <=0 : ai_needed = MIN_PLAYERS - len(game.get('players',[])) # ensure at least min if only 1 human
        for _ in range(max(0, ai_needed)): game_state_manager.add_ai_player_to_game(game_chat_id_for_logic)
        num_players = len(game.get('players', [])) + len(game.get('ai_players', []))
        player_list_str = format_player_list_html(game)
        cancel_job(context, game.get('join_end_job_name')); cancel_job(context, game.get('join_reminder_job_name'))
        if game.get('join_message_id'): 
            try: await context.bot.delete_message(game_chat_id_for_logic, game['join_message_id'])
            except TelegramError: pass
        txt = (f"Switched to Solo Mode. AI players added to meet minimum.\n\n"
               f"<b>Players ({num_players}/{MAX_PLAYERS}):</b>\n{player_list_str}\nChoose option:")
        kbd = keyboards.get_setup_phase_keyboard(num_players >= MIN_PLAYERS, True, num_players, MAX_PLAYERS, MIN_PLAYERS)
        try: # This edits the message that had "Force Solo"
            if query.message.photo: await query.edit_message_caption(caption=txt, reply_markup=kbd, parse_mode=ParseMode.HTML)
            else: await query.edit_message_text(txt, reply_markup=kbd, parse_mode=ParseMode.HTML)
        except TelegramError as e: logger.error(f"Error editing for setup_force_solo: {e}")
        game['phase'] = GAME_PHASES["SETUP"] # Revert to setup to allow further AI addition if desired
        return

    elif data == "leaderboard_personal_stats":
        stats = db.get_player_stats(user.id, game_chat_id_for_logic)
        if stats:
            # Calculate win rate, handling division by zero
            win_rate = (stats['games_won'] / stats['games_played'] * 100) if stats['games_played'] > 0 else 0
            # Calculate average score, handling division by zero
            avg_score = (stats['total_score'] / stats['games_played']) if stats['games_played'] > 0 else 0
            
            response_text = (
                f"<b>🏆 Your Personal Stats for this Chat 🏆</b>\n\n"
                f"👤 Name: {escape_html(stats['player_name'])}\n"
                f" played: {stats['games_played']}\n"
                f"🏅 Games Won: {stats['games_won']}\n"
                f"📈 Win Rate: {win_rate:.2f}%\n"
                f"📉 Average Score: {avg_score:.2f}"
            )
        else:
            response_text = "You haven't played any games in this chat yet. Start a /newgame to get on the board!"
        
        await query.message.reply_text(response_text, parse_mode=ParseMode.HTML)
        return

    elif data == "leaderboard_chat_top_5":
        leaderboard = db.get_leaderboard(game_chat_id_for_logic, limit=5)
        if leaderboard:
            response_text_parts = ["<b>📊 Top 5 Players in this Chat 📊</b>\n(Ordered by wins, then lowest average score)\n"]
            for i, stats in enumerate(leaderboard):
                avg_score = (stats['total_score'] / stats['games_played']) if stats['games_played'] > 0 else 0
                response_text_parts.append(
                    f"{i+1}. {escape_html(stats['player_name'])} - Wins: {stats['games_won']}, Avg Score: {avg_score:.2f}"
                )
            response_text = "\n".join(response_text_parts)
        else:
            response_text = "No games have been recorded in this chat yet!"

        await query.message.reply_text(response_text, parse_mode=ParseMode.HTML)
        return

    elif data == "main_menu_return":
        # This handles the "Back to Main Menu" button
        start_menu_text = "Use the buttons below or the command menu to navigate:"
        start_menu_keyboard = keyboards.get_start_keyboard()
        try:
            # Edit the media back to the main menu photo
            await query.edit_message_media(
                media=InputMediaPhoto(media=DEFAULT_GANGSTER_IMAGE, caption=start_menu_text, parse_mode=ParseMode.HTML),
                reply_markup=start_menu_keyboard
            )
        except TelegramError as e:
            logger.error(f"Error editing message for main_menu_return: {e}")
        return

    # === Game Flow Callbacks ===
    elif data == "flow_al_capone_continue":
        if not game or game['phase'] != GAME_PHASES["WAIT_FOR_AL_CAPONE_CONTINUE"] or user.id != game.get('al_capone_player_id'): 
            await query.answer("Not for you or not the right time.", show_alert=True); return
        try: await query.edit_message_text("Al Capone gives the nod... Dealing cards!", reply_markup=None, parse_mode=ParseMode.HTML)
        except TelegramError: pass
        await process_cards_deal_and_viewing_start(game_chat_id_for_logic, context)
        return

    elif data.startswith("viewing_select_card_"):
        try:
            card_idx = int(data.split("_")[-1])

            if not game: 
                logger.error(f"VIEWING_CB: Game None for G_C:{game_chat_id_for_logic}.")
                await query.answer("Game context error.", True)
                return
            
            if game['phase'] != GAME_PHASES["VIEWING"]: 
                await query.answer("Not viewing phase.", True)
                return
            
            player_data = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
            if not player_data or player_data.get('is_ai'): 
                await query.answer("Not for you.", True)
                return
            
            if len(player_data.get('viewed_card_indices', set())) < INITIAL_CARDS_TO_VIEW and \
               card_idx not in player_data.get('viewed_card_indices', set()) and \
               0 <= card_idx < len(player_data.get('hand',[])):
            
                player_data.setdefault('viewed_card_indices', set()).add(card_idx)
                card_info = player_data['hand'][card_idx]
            
                viewing_msg_id = player_data.get('viewing_message_id')
                current_pm_chat_id = query.message.chat.id 

                original_instruction_text = f"Your {len(player_data.get('hand',[]))} card(s). View any {INITIAL_CARDS_TO_VIEW} (if available)."

                temp_reveal_text = (f"👁️‍🗨️ Card #{card_idx+1}: <b>{card_info['name']}</b> ({card_info.get('points', card_info.get('value', '?'))} pts)\n"
                                    f"<i>(This message will revert in 5 seconds)</i>\n"
                                    f"({len(player_data.get('viewed_card_indices', set()))}/{INITIAL_CARDS_TO_VIEW} cards viewed)")
                
            if viewing_msg_id:
                try:
                    await context.bot.edit_message_text(
                        chat_id=current_pm_chat_id,
                        message_id=viewing_msg_id,
                        text=temp_reveal_text,
                        reply_markup=keyboards.get_card_viewing_keyboard(
                            player_data['hand'], 
                            player_data.get('viewed_card_indices', set()), 
                            INITIAL_CARDS_TO_VIEW, 
                            HAND_CARDS_COUNT
                        ),
                        parse_mode=ParseMode.HTML
                    )
                    
                    job_name = f"clear_view_{user.id}_{viewing_msg_id}_{card_idx}_{int(time.time())}"                    # Cancel any PREVIOUS clear job for THIS message_id to avoid conflicts if user clicks fast
                    
                    if player_data.get('viewing_clear_job_name'):
                        cancel_job(context, player_data['viewing_clear_job_name'])
                    player_data['viewing_clear_job_name'] = job_name 
                    # --- End of Correction for Issue 3 ---

                    context.job_queue.run_once(
                        clear_temp_card_view_job, 
                        5, 
                        data={'pm_chat_id': current_pm_chat_id, 
                                  'message_id': viewing_msg_id, 
                                  'game_chat_id': game_chat_id_for_logic, 
                                  'player_id': user.id,
                                  'original_instruction_text': original_instruction_text,
                                  'expected_job_name': job_name # Pass the job name for the check
                                  }, 
                        name=job_name
                    )
                    logger.debug(f"Temporary card reveal shown for P:{user.id}, CardIdx:{card_idx}. Clear job: {job_name}. Stored in player_data.")

                except TelegramError as e:
                    logger.warning(f"Could not edit card viewing PM for temp reveal {user.id}: {e}")
                    await query.answer(f"Card #{card_idx+1}: {card_info['name']} ({card_info.get('points', '?')} pts) - Remember it!", show_alert=True)
                
            await check_all_players_viewed_cards(game_chat_id_for_logic, context)

        except (ValueError, IndexError) as e:
            logger.error(f"VIEWING_CB: Error parsing card_idx from data '{data}': {e}")
            await query.answer("Error processing selection.", show_alert=True)
            return
        except Exception as e:
            logger.error(f"VIEWING_CB: Unhandled error: {e}", exc_info=True) # Added exc_info
            await query.answer("An error occurred.", show_alert=True)
        return

    elif data == "viewing_confirm_done":
        if not game or game['phase'] != GAME_PHASES["VIEWING"]: 
            await query.answer("Not in viewing phase or game ended.", show_alert=True); return
        
        player_data = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        if not player_data or player_data.get('is_ai'): 
            await query.answer("This action is not for you.", show_alert=True); return

        logger.info(f"Player {user.id} clicked 'Done Viewing' for game {game_chat_id_for_logic}.")

        if len(player_data.get('viewed_card_indices', set())) < INITIAL_CARDS_TO_VIEW:
            await query.answer(f"Please view {INITIAL_CARDS_TO_VIEW} cards first before confirming you're done.", show_alert=True)
            return

        player_data['viewed_all_initial_cards'] = True # CRITICAL: Set this flag
        logger.debug(f"Player {user.id} confirmed done viewing. viewed_all_initial_cards set to True.")
        
        viewing_msg_id = player_data.get('viewing_message_id')
        current_pm_chat_id = query.message.chat.id # User's PM chat
        if viewing_msg_id:
            if player_data.get('viewing_clear_job_name'): # Cancel pending text revert job
                cancel_job(context, player_data['viewing_clear_job_name'])
                player_data['viewing_clear_job_name'] = None
            try:
                await context.bot.edit_message_text(
                    chat_id=current_pm_chat_id, message_id=viewing_msg_id,
                    text="Thanks for viewing your cards! Waiting for other players or the main timer.",
                    reply_markup=None # Clear buttons
                )
            except TelegramError as e: logger.warning(f"Error editing 'Done Viewing' PM for {user.id}: {e}")
        
        await check_all_active_humans_done_viewing_and_proceed(game_chat_id_for_logic, context)    
        return

    # === Player Turn Action Callbacks ===
    elif data.startswith("turn_call_omerta_"):
        expected_player_id = int(data.split("_")[-1])
        if not game or game['phase'] != GAME_PHASES["PLAYING"] or user.id != expected_player_id or user.id != game.get('current_player_id'): 
            await query.answer("Not your turn or invalid action.", show_alert=True); return
        player_data = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        if player_data.get('cannot_call_omerta'): await query.answer("Mamma says no Omerta for you this turn!", show_alert=True); return
        try: await query.edit_message_reply_markup(reply_markup=None) 
        except TelegramError: pass # Message might be gone
        await handle_omerta_call(game_chat_id_for_logic, context, user.id)
        return

    elif data.startswith("turn_draw_deck_"):
        expected_player_id = int(data.split("_")[-1])
        if not game or game['phase'] != GAME_PHASES["PLAYING"] or user.id != expected_player_id or user.id != game.get('current_player_id'):
            await query.answer("Not your turn or invalid action.", show_alert=True); return
        try: await query.edit_message_reply_markup(reply_markup=None)
        except TelegramError: pass
        await handle_player_action_draw_deck(game, context, user.id)
        return

    elif data.startswith("turn_draw_discard_"):
        expected_player_id = int(data.split("_")[-1])
        if not game or game['phase'] != GAME_PHASES["PLAYING"] or user.id != expected_player_id or user.id != game.get('current_player_id'):
            await query.answer("Not your turn or invalid action.", show_alert=True); return
        try: await query.edit_message_reply_markup(reply_markup=None)
        except TelegramError: pass
        await handle_player_action_draw_discard(game, context, user.id) 
        return
    
    # Card replacement after drawing - Format: replace_hand_card_select_{idx}_{player_id}
    elif data.startswith("replace_hand_card_select_"):
        parts = data.split("_"); expected_player_id = int(parts[-1]); card_idx_to_replace = int(parts[-2])
        if not game or game['phase'] != GAME_PHASES["PLAYING"] or user.id != expected_player_id or user.id != game.get('current_player_id'):
            await query.answer("Invalid action for card replacement.", show_alert=True); return
        turn_ctx = game.get('player_turn_context', {}).get(user.id, {})
        drawn_card_info = turn_ctx.get('drawn_card')
        source_of_draw = turn_ctx.get('draw_source')
        if drawn_card_info is None:
            await query.answer("Error: No drawn card context found. Your turn might reset.", show_alert=True)
            # Resend turn options to player
            current_player_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
            if current_player_obj: # Ensure current_player_obj is not None
                is_f_cycle_ac = (game['cycle_count'] == 1 and user.id == game.get('al_capone_player_id'))
                action_kbd = keyboards.get_player_turn_actions_keyboard(game, current_player_obj, is_f_cycle_ac)
                await send_message_to_player(context, user.id, "Error in previous action. It's still your turn:", reply_markup=action_kbd)
            return
        try: await query.edit_message_reply_markup(reply_markup=None)
        except TelegramError: pass
        await process_card_replacement(game, context, user.id, card_idx_to_replace, drawn_card_info, source_of_draw)
        if game.get('player_turn_context') and user.id in game['player_turn_context']: del game['player_turn_context'][user.id]
        return

    # Cancel card replacement - Format: replace_hand_card_cancel_overall_{player_id}
    elif data.startswith("replace_hand_card_cancel_overall_"):
        expected_player_id = int(data.split("_")[-1])
        if not game or game['phase'] != GAME_PHASES["PLAYING"] or user.id != expected_player_id or user.id != game.get('current_player_id'):
            await query.answer("Invalid cancel action.", show_alert=True); return
        drawn_card_info = game.get('player_turn_context', {}).get(user.id, {}).get('drawn_card')
        try: 
            cancel_text = "Card replacement cancelled. Your hand is unchanged."
            if drawn_card_info: game['discard_pile'].append(drawn_card_info); cancel_text += f" The drawn card ({drawn_card_info['name']}) was discarded."
            await query.edit_message_text(cancel_text, reply_markup=None)
        except TelegramError: pass
        if drawn_card_info and game.get('player_turn_context') and user.id in game['player_turn_context']: 
             await context.bot.send_message(game_chat_id_for_logic, f"{get_player_mention(game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id))} cancelled replacement. Drawn card {drawn_card_info['name']} discarded.", parse_mode=ParseMode.HTML)
        if game.get('player_turn_context') and user.id in game['player_turn_context']: del game['player_turn_context'][user.id]
        current_player_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        is_f_cycle_ac = (game['cycle_count'] == 1 and user.id == game.get('al_capone_player_id'))
        action_kbd = keyboards.get_player_turn_actions_keyboard(game, current_player_obj, is_f_cycle_ac)
        await send_message_to_player(context, user.id, "Your turn continues. Choose an action:", reply_markup=action_kbd)
        return

    # Button 4: Match discarded bottle on own turn - Format: turn_match_discarded_bottle_{player_id}
    elif data.startswith("turn_match_discarded_bottle_"):
        expected_player_id = int(data.split("_")[-1])
        if not game or game['phase'] != GAME_PHASES["PLAYING"] or user.id != expected_player_id or user.id != game.get('current_player_id'):
            await query.answer("Not your turn or invalid action.", show_alert=True); return
        player_data = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        if not game['discard_pile'] or game['discard_pile'][-1].get('type') != 'bottle':
            await send_message_to_player(context, user.id, "No Bottle on discard to match now.")
            try: await query.edit_message_reply_markup(reply_markup=None) # Clear old turn options
            except TelegramError: pass
            is_f_cycle_ac = (game['cycle_count'] == 1 and user.id == game.get('al_capone_player_id'))
            action_kbd = keyboards.get_player_turn_actions_keyboard(game, player_data, is_f_cycle_ac)
            await send_message_to_player(context, user.id, "Choose your action:", reply_markup=action_kbd)
            return
        discarded_bottle_to_match = game['discard_pile'][-1]
        game.setdefault('player_turn_context', {})[user.id] = {'action': 'turn_bottle_match', 'bottle_to_match_value': discarded_bottle_to_match.get('value')}
        blocked_indices = set(game.get('blocked_cards', {}).get(user.id, {}).keys())
        try:
            await query.edit_message_text(
                f"You want to match <b>{escape_html(discarded_bottle_to_match['name'])}</b>.\nChoose YOUR card (facedown by position) to attempt match:",
                reply_markup=keyboards.get_card_selection_keyboard(
                    "player_turn_chose_card_for_bottle_match", player_data['hand'], user.id, facedown=True, num_to_select=1, 
                    allow_cancel=True, cancel_callback_data=f"player_turn_chose_card_for_bottle_match_cancel_overall_{user.id}",
                    blocked_card_indices=blocked_indices
                ), parse_mode=ParseMode.HTML
            )
        except TelegramError as e: logger.error(f"Error editing for Button 4 prompt: {e}")
        return

    # Player chose card for Button 4 match - Format: player_turn_chose_card_for_bottle_match_select_{idx}_{player_id}
    elif data.startswith("player_turn_chose_card_for_bottle_match_select_"):
        parts = data.split("_"); expected_player_id = int(parts[-1]); card_idx_chosen = int(parts[-2])
        if not game or game['phase'] != GAME_PHASES["PLAYING"] or user.id != expected_player_id or user.id != game.get('current_player_id'):
            await query.answer("Invalid action.", show_alert=True); return
        turn_ctx = game.get('player_turn_context', {}).get(user.id, {})
        if turn_ctx.get('action') != 'turn_bottle_match': await query.answer("Invalid action sequence.", show_alert=True); return
        player_data = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        if not (0 <= card_idx_chosen < len(player_data['hand'])): await query.answer("Invalid card selected.", show_alert=True); return
        card_player_chose = player_data['hand'][card_idx_chosen]
        value_to_match = turn_ctx['bottle_to_match_value']
        try: await query.edit_message_reply_markup(reply_markup=None)
        except TelegramError: pass

        # --- Success Path (This logic is being changed) ---
        if card_player_chose.get('type') == 'bottle' and card_player_chose.get('value') == value_to_match:
            matched_card = player_data['hand'].pop(card_idx_chosen)
            game['discard_pile'].append(matched_card)
            await send_message_to_player(context, user.id, f"Success! You matched and discarded {escape_html(matched_card['name'])}.")
            await context.bot.send_message(game_chat_id_for_logic, f"🎯 {get_player_mention(player_data)} (on turn) matched bottle, discarded <b>{escape_html(matched_card['name'])}</b>!", parse_mode=ParseMode.HTML)
            if game.get('player_turn_context') and user.id in game['player_turn_context']: del game['player_turn_context'][user.id]

            # --- NEW LOGIC FOR SUCCESS ---
            # Instead of processing the new discard, we return the player to their turn options.
            logger.info(f"Player {user.id} succeeded in turn-based match. Re-sending turn options.")
            is_f_cycle_ac = (game['cycle_count'] == 1 and user.id == game.get('al_capone_player_id'))
            action_kbd = keyboards.get_player_turn_actions_keyboard(game, player_data, is_f_cycle_ac)
            await send_message_to_player(context, user.id, "Your turn continues. Choose another action:", reply_markup=action_kbd)
            # --- END OF NEW LOGIC ---

        # --- Failure Path (This logic is already correct from our previous fix) ---
        else:
            # This part, which shows the card they *tried* to use, is fine.
            penalty_msg_player = f"Oops! Card #{card_idx_chosen+1} (<b>{escape_html(card_player_chose['name'])}</b>) is not Bottle {value_to_match}."
            penalty_msg_group = f"⚠️ {get_player_mention(player_data)} tried to match with Card #{card_idx_chosen+1} (<b>{escape_html(card_player_chose['name'])}</b>) on turn, but failed!"

            # This part, which hides the penalty card, is also fine.
            if game['deck']:
                penalty_card = game['deck'].pop()
                player_data['hand'].append(penalty_card)
                penalty_msg_player += "\nYou have received a penalty card, drawn facedown from the deck."
                penalty_msg_group += " A penalty card was drawn."
                logger.info(f"Player {user.id} drew penalty card '{penalty_card.get('name')}' (hidden from all players).")
            else:
                penalty_msg_player += "\nDeck empty, no penalty."
                penalty_msg_group += " Deck was empty, so no penalty card was drawn."

            await send_message_to_player(context, user.id, penalty_msg_player, parse_mode=ParseMode.HTML)
            await context.bot.send_message(game_chat_id_for_logic, penalty_msg_group, parse_mode=ParseMode.HTML)
            if game.get('player_turn_context') and user.id in game['player_turn_context']: del game['player_turn_context'][user.id]

            # Re-send turn options
            logger.info(f"Player {user.id} failed turn-based match. Re-sending turn options.")
            is_f_cycle_ac = (game['cycle_count'] == 1 and user.id == game.get('al_capone_player_id'))
            action_kbd = keyboards.get_player_turn_actions_keyboard(game, player_data, is_f_cycle_ac)
            await send_message_to_player(context, user.id, "Your turn continues. Choose another action:", reply_markup=action_kbd)

        return # We add a return here to ensure no other logic in the handler is accidentally triggered.

    # Cancel Button 4 match attempt - Format: player_turn_chose_card_for_bottle_match_cancel_overall_{player_id}
    elif data.startswith("player_turn_chose_card_for_bottle_match_cancel_overall_"):
        expected_player_id = int(data.split("_")[-1])
        if not game or game['phase'] != GAME_PHASES["PLAYING"] or user.id != expected_player_id or user.id != game.get('current_player_id'): return
        if game.get('player_turn_context') and user.id in game['player_turn_context']: del game['player_turn_context'][user.id]
        try: await query.edit_message_text("Bottle match attempt cancelled. Choose another action.", reply_markup=None)
        except TelegramError: pass
        player_data = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        is_f_cycle_ac = (game['cycle_count'] == 1 and user.id == game.get('al_capone_player_id'))
        action_kbd = keyboards.get_player_turn_actions_keyboard(game, player_data, is_f_cycle_ac)
        await send_message_to_player(context, user.id, "It's still your turn:", reply_markup=action_kbd)
        return

    # (Inside handle_callback_query)
    # === Global Bottle Matching Window Callbacks ===
    # Format: bottle_match_do_discard_{card_idx}_{player_id}
    # Format: bottle_match_skip_own_{player_id}
    elif data.startswith("bottle_match_do_discard_"): 
        if not game or game.get('phase') != GAME_PHASES["BOTTLE_MATCHING_WINDOW"]: 
            try: await query.answer("Bottle matching window is closed.", show_alert=True)
            except TelegramError: pass; return
        parts = data.split("_"); card_idx_to_discard = int(parts[-2]); expected_player_id = int(parts[-1])
        if user.id != expected_player_id: 
            await query.answer("This match option was for another player.", show_alert=True); return
        
        logger.info(f"CBQ: User {user.id} attempting bottle match with card index {card_idx_to_discard} from CBQ.")
        await handle_bottle_match_attempt(game, context, user.id, card_idx_to_discard, is_ai_attempt=False)
        return

    elif data.startswith("bottle_match_skip_own_"): 
        if not game or game.get('phase') != GAME_PHASES["BOTTLE_MATCHING_WINDOW"]: return # Silently ignore if window closed
        expected_player_id = int(data.split("_")[-1])
        if user.id != expected_player_id: return
        try: 
            await query.answer("You chose not to match this time.", show_alert=False)
            await query.edit_message_text("You skipped matching the bottle for now.", reply_markup=None)
        except TelegramError: pass # Message might already be cleared by window ending
        return

    # === Character Ability Callbacks ===
    # Each ability will have a block like this.
    # --- The Mole ---
    # Callback: ability_mole_select_{idx}_{player_id}
    elif data.startswith("ability_mole_do_peek_"):
    # Expected format: "ability_mole_do_peek_{idx}_{player_id}"
        parts = data.split("_")
        try:
            player_id_from_cb = int(parts[-1])
            card_idx_to_view = int(parts[-2])
        except (ValueError, IndexError) as e:
            logger.error(f"Mole Peek CB: Error parsing '{data}': {e}")
            await query.answer("Error processing Mole action.", show_alert=True)
            return

        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb or \
        active_ability_ctx.get('card_name') != "The Mole" or active_ability_ctx.get('step') != 'mole_select_own_card':
            await query.answer("Not a valid Mole action for you now.", show_alert=True); return
        
        cancel_job(context, active_ability_ctx.get('timeout_job_name')) 
        try: await query.edit_message_reply_markup(reply_markup=None) 
        except TelegramError: pass
        await execute_the_mole_ability(game, context, user.id, card_idx_to_view) # 'game' object passed
        return

   # --- The Lady ---
    elif data.startswith("ability_lady_target_select_target_"):
        # Validate context first
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Lady" or active_ability_ctx.get('step') != 'lady_select_target':
            await query.answer("Not a valid Lady action for you now.", show_alert=True); return

        # --- Standardized ID Parsing Logic ---
        prefix = "ability_lady_target_select_target_"
        target_id_str = data[len(prefix):]
        target_id: Union[int, str] = int(target_id_str) if target_id_str.isdigit() else target_id_str
        # --- End of Standardized Logic ---
        
        active_ability_ctx['targets_chosen'] = [target_id]
        active_ability_ctx['step'] = 'lady_confirm_target'
        
        target_player_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, target_id)
        if not target_player_obj:
            logger.error(f"Lady Target Error: Player '{target_id}' not found. CBQ data: {data}")
            await query.answer("Selected target not found. Please try again.", show_alert=True)
            # Reset step to allow re-selection
            active_ability_ctx['step'] = 'lady_select_target'
            active_ability_ctx['targets_chosen'] = []
            return

        # Use the now-correctly-parsed target_id in callbacks
        confirm_cb = f"ability_lady_final_confirm_{target_id}" 
        cancel_cb = f"ability_lady_overall_cancel_{user.id}"
        try:
            await query.edit_message_text(
                f"You chose The Lady for {get_player_mention(target_player_obj)}. Are you sure?",
                reply_markup=keyboards.get_confirmation_keyboard(confirm_cb, cancel_cb), parse_mode=ParseMode.HTML )
        except TelegramError as e: logger.error(f"Error editing for Lady target confirm: {e}")
        return

    elif data.startswith("ability_lady_final_confirm_"):
        if not active_ability_ctx or \
           active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Lady" or \
           active_ability_ctx.get('step') != 'lady_confirm_target':
            await query.answer("Not a valid Lady confirmation for you at this time.", show_alert=True)
            return

        # --- Standardized ID Parsing Logic ---
        prefix = "ability_lady_final_confirm_"
        target_id_str = data[len(prefix):]
        target_id: Union[int, str] = int(target_id_str) if target_id_str.isdigit() else target_id_str
        # --- End of Standardized Logic ---

        if not active_ability_ctx.get('targets_chosen') or active_ability_ctx['targets_chosen'][0] != target_id:
            logger.warning(f"Lady Final Confirm: Target ID mismatch. Context: {active_ability_ctx.get('targets_chosen')}, CBQ parsed: {target_id}.")
            await query.answer("Target mismatch. Please try the action again.", show_alert=True)
            return
        
        original_context_signature = id(active_ability_ctx)
        lady_ctx_snapshot = copy.deepcopy(active_ability_ctx)
        
        killer_initiated = await check_for_killer_reaction(
            game_chat_id_for_logic, context, user.id, target_id, "The Lady", original_context_signature
        )

        if killer_initiated:
            try: 
                target_player_obj_mention = get_player_mention(game_state_manager.get_player_by_id(game_chat_id_for_logic, target_id))
                await query.edit_message_text(f"Targeting {target_player_obj_mention} with The Lady. Waiting for their reaction...", reply_markup=None, parse_mode=ParseMode.HTML)
            except TelegramError as e:
                logger.error(f"Lady Final Confirm: Error editing message for Killer prompt: {e}")
        else: 
            cancel_job(context, lady_ctx_snapshot.get('timeout_job_name'))
            try: 
                await query.edit_message_text("The Lady works her charm...", reply_markup=None, parse_mode=ParseMode.HTML)
            except TelegramError as e:
                logger.error(f"Lady Final Confirm: Error editing message for Lady execution: {e}")
            
            game['active_ability_context'] = lady_ctx_snapshot 
            await execute_the_lady_ability(game, context, user.id, target_id) 
        return
    
    # --- The Mamma ---
    elif data.startswith("ability_mamma_target_select_target_"):
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Mamma" or active_ability_ctx.get('step') != 'mamma_select_target':
            await query.answer("Not a valid Mamma action.", show_alert=True)
            return

        # --- Standardized ID Parsing Logic ---
        prefix = "ability_mamma_target_select_target_"
        target_id_str = data[len(prefix):]
        target_id: Union[int, str] = int(target_id_str) if target_id_str.isdigit() else target_id_str
        # --- End of Standardized Logic ---

        active_ability_ctx['targets_chosen'] = [target_id]
        active_ability_ctx['step'] = 'mamma_confirm_target'
        
        target_player_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, target_id)
        if not target_player_obj:
            logger.error(f"Mamma Target Error: Player {target_id} not found. CBQ data: {data}")
            await query.answer("Selected target not found.", show_alert=True)
            return

        confirm_cb = f"ability_mamma_final_confirm_{target_id}"
        cancel_cb = f"ability_mamma_overall_cancel_{user.id}"
        try:
            await query.edit_message_text(f"Use The Mamma on {get_player_mention(target_player_obj)}?", 
                                          reply_markup=keyboards.get_confirmation_keyboard(confirm_cb, cancel_cb), 
                                          parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.error(f"Err edit Mamma confirm: {e}")
        return

    elif data.startswith("ability_mamma_final_confirm_"):
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Mamma" or active_ability_ctx.get('step') != 'mamma_confirm_target':
            await query.answer("Not a valid Mamma confirmation.", show_alert=True); return

        # --- Standardized ID Parsing Logic ---
        prefix = "ability_mamma_final_confirm_"
        target_id_str = data[len(prefix):]
        target_id: Union[int, str] = int(target_id_str) if target_id_str.isdigit() else target_id_str
        # --- End of Standardized Logic ---
        
        if not active_ability_ctx.get('targets_chosen') or active_ability_ctx['targets_chosen'][0] != target_id:
            await query.answer("Target mismatch. The action may have been cancelled.",True); return

        original_context_signature = id(active_ability_ctx)
        mamma_ctx_snapshot = copy.deepcopy(active_ability_ctx)

        killer_initiated = await check_for_killer_reaction(game_chat_id_for_logic, context, user.id, target_id, "The Mamma", original_context_signature)
        
        if killer_initiated:
            try: 
                target_mention = get_player_mention(game_state_manager.get_player_by_id(game_chat_id_for_logic, target_id))
                await query.edit_message_text(f"Targeting {target_mention} with Mamma. Waiting for their counter...", reply_markup=None, parse_mode=ParseMode.HTML)
            except TelegramError: pass
        else:
            cancel_job(context, mamma_ctx_snapshot.get('timeout_job_name'))
            try: 
                await query.edit_message_text("Mamma has spoken...", reply_markup=None, parse_mode=ParseMode.HTML)
            except TelegramError: pass
            
            game['active_ability_context'] = mamma_ctx_snapshot
            await execute_the_mamma_ability(game, context, user.id, target_id)
        return

    # --- The Driver ---
    elif data.startswith("ability_driver_card_select_") or data.startswith("ability_driver_card_deselect_"):
        parts = data.split("_"); player_id_from_cb = int(parts[-1]); card_idx = int(parts[-2])
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb or \
           active_ability_ctx.get('card_name') != "The Driver" or active_ability_ctx.get('step') != 'driver_select_cards':
            await query.answer("Not a valid Driver action.", show_alert=True); return
        
        player_data = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        if not player_data: return

        selected_indices = active_ability_ctx.get('cards_selected_indices', [])
        if data.startswith("ability_driver_card_select_"):
            if card_idx not in selected_indices and len(selected_indices) < 2: selected_indices.append(card_idx)
            elif len(selected_indices) >= 2: await query.answer("Max 2 cards for Driver.", show_alert=True)
        else: # is deselecting
            if card_idx in selected_indices: selected_indices.remove(card_idx)
        
        active_ability_ctx['cards_selected_indices'] = selected_indices
        
        # --- THIS IS THE FIX ---
        # We must fetch the blocked indices here before redrawing the keyboard
        blocked_indices = set(game.get('blocked_cards', {}).get(str(user.id), {}).keys())
        
        kbd = keyboards.get_card_selection_keyboard(
            "ability_driver_card", player_data['hand'], user.id, True, 2, 1, 
            selected_indices, True, f"ability_driver_overall_cancel_{user.id}",
            blocked_card_indices=blocked_indices  # Pass the blocked indices
        )
        # --- END OF FIX ---

        sel_count = len(selected_indices)
        instr = "The Driver: Select 1 or 2 Bottles (facedown).\n"
        if sel_count == 0: instr += "Select at least one."
        elif sel_count == 1: instr += "Selected 1. Select another or Confirm."
        else: instr += "Selected 2. Confirm."

        try: 
            await query.edit_message_text(instr, reply_markup=kbd)
        except TelegramError as e: 
            logger.error(f"Err edit Driver select: {e}")
        return

    elif data.startswith("ability_driver_card_confirm_"): 
        player_id_from_cb = int(data.split("_")[-1])
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb or \
           active_ability_ctx.get('card_name') != "The Driver" or active_ability_ctx.get('step') != 'driver_select_cards':
            await query.answer("Not valid Driver confirm.",True); return
        selected_indices = active_ability_ctx.get('cards_selected_indices', [])
        if not (1 <= len(selected_indices) <= 2):
            await query.answer("Must select 1 or 2 cards.",True); return

        cancel_job(context, active_ability_ctx.get('timeout_job_name'))
        try: await query.edit_message_text("Vroom! The Driver makes a drop...", reply_markup=None, parse_mode=ParseMode.HTML)
        except TelegramError: pass

        await execute_the_driver_ability(game, context, user.id, selected_indices)
        return 


   # === Generic Ability Overall Cancel ===
    # Format: ability_{ability_name_lower}_overall_cancel_{player_id}
    # Example: ability_mamma_overall_cancel_341007979
    elif data.startswith("ability_") and "overall_cancel" in data: # Make check more specific
        # ability_mamma_overall_cancel_341007979 -> parts: ["ability", "mamma", "overall", "cancel", "341007979"]
        parts = data.split("_")
        try:
            player_id_from_cb_str = parts[-1]
            player_id_from_cb = int(player_id_from_cb_str)
            # Try to reconstruct ability name for a more precise check, though not strictly necessary if player_id is the main guard
            # ability_name_from_cb_parts = parts[1:-3] # e.g., ["mamma"] or ["police", "patrol"]
            # reconstructed_ability_name = " ".join(part.capitalize() for part in ability_name_from_cb_parts)
        except (ValueError, IndexError):
            logger.warning(f"Could not parse player_id from generic cancel CBQ: {data}")
            await query.answer("Invalid cancel format.", True); return

        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb :
            await query.answer("Not your active ability to cancel or context missing.", show_alert=True); return

        ability_name_in_ctx = active_ability_ctx.get('card_name', 'Unknown Ability')
        # Optional: More precise check
        # if reconstructed_ability_name and ability_name_in_ctx != reconstructed_ability_name:
        #     logger.warning(f"Generic cancel: Mismatch between CBQ ability name guess '{reconstructed_ability_name}' and context '{ability_name_in_ctx}'. Player ID matched.")
        #     await query.answer("Ability context mismatch for cancel.", show_alert=True); return

        logger.info(f"Player {user.id} cancelled ability {ability_name_in_ctx} (CBQ: {data}). Step: {active_ability_ctx.get('step')}")
        if active_ability_ctx.get('timeout_job_name'):
            cancel_job(context, active_ability_ctx['timeout_job_name'])
        game['active_ability_context'] = None

        try:
            msg_text_edited = f"You cancelled using {escape_html(ability_name_in_ctx)}."
            target_msg = query.message
            if target_msg: # Ensure message exists
                if target_msg.text and hasattr(target_msg, 'edit_text'): await query.edit_message_text(msg_text_edited, reply_markup=None, parse_mode=ParseMode.HTML)
                elif target_msg.caption and hasattr(target_msg, 'edit_caption'): await query.edit_message_caption(caption=msg_text_edited, reply_markup=None, parse_mode=ParseMode.HTML)
                else: await send_message_to_player(context, user.id, msg_text_edited) # Fallback to new message
            else: await send_message_to_player(context, user.id, msg_text_edited)
        except TelegramError as e:
            logger.warning(f"Error editing PM for ability cancel: {e}")


        player_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        try:
            if player_obj: # Ensure player_obj exists before trying to mention
                await context.bot.send_message(game_chat_id_for_logic, f"{get_player_mention(player_obj)} cancelled their {escape_html(ability_name_in_ctx)} action.", parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.error(f"Error sending group message for ability cancel: {e}")

        await advance_turn_or_continue_sequence(game, context)
        return

    # === Fallback / No Action ===
    elif data.startswith("no_action_dummy"): await query.answer("This option is not currently available.", show_alert=True)
    elif data.startswith("no_action_"):
        # ... existing no_action logic ...
        pass
    else:
        # This is the fallback for truly unhandled data strings
        if game: # Check if game context still exists
            logger.warning(f"Unhandled CBQ (end of func WITH game): {data} Ph:{game.get('phase')} U:{user.id} CtxStep:{active_ability_ctx.get('step') if active_ability_ctx else 'N/A'}")
        else:
            logger.warning(f"Unhandled CBQ (end of func NO game): {data} U:{user.id}")
        try:
            await query.answer("This action is not recognized or is outdated.", show_alert=True)
        except TelegramError: pass
        
    # --- The Snitch Callbacks ---
    # ability_snitch_target_select_target_{target_id}
    # ability_snitch_target_deselect_target_{target_id}
    # ability_snitch_target_confirm_targets -> then Killer check if 1 target
    # ability_snitch_target_overall_cancel_{player_id} (maps to generic overall cancel)

    # In simple_bot.py, inside handle_callback_query()

    # --- The Snitch Callbacks --- (This whole block is being replaced)
    if data.startswith("ability_snitch_target_select_target_") or data.startswith("ability_snitch_target_deselect_target_"):
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Snitch" or active_ability_ctx.get('step') != 'snitch_select_targets':
            await query.answer("Not a valid Snitch action for you now.", show_alert=True); return

        # --- Standardized ID Parsing Logic for Select/Deselect ---
        is_selecting = data.startswith("ability_snitch_target_select_target_")
        prefix = "ability_snitch_target_select_target_" if is_selecting else "ability_snitch_target_deselect_target_"
        target_id_str = data[len(prefix):]
        target_id: Union[int, str] = int(target_id_str) if target_id_str.isdigit() else target_id_str
        # --- End of Standardized Logic ---

        selected_targets = active_ability_ctx.get('targets_chosen', [])
        max_snitch_targets = 2

        if is_selecting:
            if target_id not in selected_targets and len(selected_targets) < max_snitch_targets:
                selected_targets.append(target_id)
            elif len(selected_targets) >= max_snitch_targets:
                await query.answer(f"You can select at most {max_snitch_targets} targets.", show_alert=True)
                return 
        else: # is_deselecting
            if target_id in selected_targets:
                selected_targets.remove(target_id)

        active_ability_ctx['targets_chosen'] = selected_targets
        
        snitch_kbd = keyboards.get_target_player_keyboard(
            game, user.id, "ability_snitch_target",
            max_targets=max_snitch_targets, min_targets=1,
            already_selected_targets=selected_targets,
            cancel_callback_data=f"ability_snitch_overall_cancel_{user.id}"
        )

        instr_text = "The Snitch: Choose 1 or 2 players to give cards to.\n"
        instr_text += f"Currently selected: {len(selected_targets)} player(s). "
        if 1 <= len(selected_targets) <= max_snitch_targets:
            instr_text += "Press Confirm when ready."
        else:
            instr_text += "Select at least 1 target."
        try:
            await query.edit_message_text(instr_text, reply_markup=snitch_kbd, parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.error(f"Error editing Snitch target selection message: {e}")
        return

    elif data.startswith("ability_snitch_target_confirm_targets"): 
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Snitch" or active_ability_ctx.get('step') != 'snitch_select_targets': 
            await query.answer("Not a valid Snitch confirmation.", show_alert=True); return
        
        selected_targets = active_ability_ctx.get('targets_chosen', [])
        if not (1 <= len(selected_targets) <= 2):   
            await query.answer("Please select 1 or 2 targets for The Snitch.", show_alert=True); return
            
        active_ability_ctx['step'] = 'snitch_confirmed_targets'     
        
        original_context_signature = id(active_ability_ctx)
        snitch_context_snapshot_for_killer = copy.deepcopy(active_ability_ctx)
        
        killer_initiated = False
        if len(selected_targets) == 1:
            target_id_for_killer_check = selected_targets[0]
            killer_initiated = await check_for_killer_reaction(
                game_chat_id_for_logic,
                context,
                user.id,
                target_id_for_killer_check,
                "The Snitch",
                original_context_signature
            )
            if killer_initiated:
                logger.info(f"Snitch confirm: Killer reaction initiated against P:{target_id_for_killer_check} by P:{user.id}.")
                try:
                    target_player_obj_mention = game_state_manager.get_player_by_id(game_chat_id_for_logic, target_id_for_killer_check)
                    await query.edit_message_text(f"Snitching on {get_player_mention(target_player_obj_mention)}. Waiting for their reaction...", reply_markup=None, parse_mode=ParseMode.HTML)
                except TelegramError: pass
                return
        
        logger.info(f"Snitch confirm: No Killer reaction, or multiple targets. Proceeding for P:{user.id}.")
        cancel_job(context, snitch_context_snapshot_for_killer.get('timeout_job_name'))
        try:
            await query.edit_message_text("The Snitch is passing information (cards)...", reply_markup=None, parse_mode=ParseMode.HTML)
        except TelegramError: pass
        
        game['active_ability_context'] = snitch_context_snapshot_for_killer
        
        await execute_the_snitch_ability(game, context, user.id, selected_targets)
        return

    # --- The Safecracker Callbacks ---
    # ability_safecracker_view_safe_{player_id} -> shows safe, then keyboard to pick safe card
    # ability_safecracker_selected_safe_card_to_take_select_{idx}_{player_id} -> chosen safe card, prompt for hand card
    # ability_safecracker_selected_hand_card_to_give_select_{idx}_{player_id} -> chosen hand card, prompt for final confirm
    # ability_safecracker_final_confirm -> execute
    # ability_safecracker_overall_cancel_{player_id} (from initial prompt)
    # ability_safecracker_selected_safe_card_to_take_cancel_step_{player_id} (cancel after viewing safe - should go back or cancel all)
    
    elif data.startswith("ability_safecracker_view_safe_"):
        player_id_from_cb = int(data.split("_")[-1])
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb or \
           active_ability_ctx.get('card_name') != "The Safecracker" or active_ability_ctx.get('step') != 'safecracker_initial_prompt':
            await query.answer("Not a valid Safecracker action now.",True); return
        active_ability_ctx['step'] = 'safecracker_choosing_safe_card_after_view'
        safe_contents_text = "<b>Safe Contents (For Your Eyes Only!):</b>\n"
        if game.get('safe') and len(game['safe']) > 0:
            for i, card_obj in enumerate(game['safe']): safe_contents_text += f"- Pos #{i+1}: {escape_html(card_obj.get('name'))} ({card_obj.get('points', '?')} pts)\n"
        else:
            safe_contents_text += "The Safe is empty! No exchange possible."
            try: await query.edit_message_text(safe_contents_text, reply_markup=None, parse_mode=ParseMode.HTML)
            except TelegramError: pass
            game['active_ability_context'] = None; await advance_turn_or_continue_sequence(game, context); return
        safe_contents_text += "\nNow, choose one Safe card position to TAKE (this will be your new card):"
        try:
            await query.edit_message_text(safe_contents_text,
                reply_markup=keyboards.get_safe_card_selection_for_exchange_keyboard(len(game['safe']), "sf_take_card", user.id),
                parse_mode=ParseMode.HTML )
        except TelegramError as e: logger.error(f"Err edit SF view: {e}")
        return

    elif data.startswith("sf_take_card_select_"):
        parts = data.split("_"); player_id_from_cb = int(parts[-1]); safe_card_idx_chosen = int(parts[-2])
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb or \
           active_ability_ctx.get('card_name') != "The Safecracker" or active_ability_ctx.get('step') != 'safecracker_choosing_safe_card_after_view':
            await query.answer("Invalid SF step.",True); return
        if not (0 <= safe_card_idx_chosen < len(game.get('safe',[]))): await query.answer("Invalid Safe card.",True); return
        active_ability_ctx['safecracker_safe_card_idx_to_take'] = safe_card_idx_chosen
        active_ability_ctx['step'] = 'safecracker_choosing_hand_card_to_give'
        player_data = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        blocked_indices = set(game.get('blocked_cards', {}).get(user.id, {}).keys())
        try:
            await query.edit_message_text(
                f"You selected Safe Position #{safe_card_idx_chosen+1}. Now, choose YOUR hand card (facedown) to place into the Safe:",
                reply_markup=keyboards.get_card_selection_keyboard(
                    "sf_give_card", player_data['hand'], user.id, True, 1, 1, None, 
                    True, f"sf_overall_cancel_{user.id}", # Cancel goes to overall
                    blocked_indices ))
        except TelegramError as e: logger.error(f"Err edit SF hand card prompt: {e}")
        return

    elif data.startswith("sf_give_card_select_"):
        parts = data.split("_"); player_id_from_cb = int(parts[-1]); hand_card_idx_chosen = int(parts[-2])
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb or \
           active_ability_ctx.get('card_name') != "The Safecracker" or active_ability_ctx.get('step') != 'safecracker_choosing_hand_card_to_give':
            await query.answer("Invalid SF step.",True); return
        safe_card_idx_to_take = active_ability_ctx.get('safecracker_safe_card_idx_to_take')
        if safe_card_idx_to_take is None: await query.answer("Err: Safe choice missing.",True); return
        if not (0 <= hand_card_idx_chosen < len(game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)['hand'])):
            await query.answer("Invalid hand card.",True); return
        active_ability_ctx['safecracker_hand_card_idx_to_give'] = hand_card_idx_chosen
        active_ability_ctx['step'] = 'safecracker_confirm_exchange'
        confirm_text = (f"Confirm exchange: Your Card #{hand_card_idx_chosen+1} (facedown) "
                        f"for Safe Position #{safe_card_idx_to_take+1} (you saw what's in the safe)?")
        try: await query.edit_message_text(confirm_text, reply_markup=keyboards.get_confirmation_keyboard(f"sf_final_confirm_{user.id}", f"sf_overall_cancel_{user.id}"))
        except TelegramError as e: logger.error(f"Err edit SF final confirm: {e}")
        
    elif data.startswith("sf_final_confirm_"):
        player_id_from_cb = int(data.split("_")[-1])
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb or \
           active_ability_ctx.get('card_name') != "The Safecracker" or active_ability_ctx.get('step') != 'safecracker_confirm_exchange':
            await query.answer("Invalid SF confirm.",True); return
        safe_idx = active_ability_ctx.get('safecracker_safe_card_idx_to_take')
        hand_idx = active_ability_ctx.get('safecracker_hand_card_idx_to_give')
        if safe_idx is None or hand_idx is None: await query.answer("Err: Exchange details incomplete.",True); return
        cancel_job(context, active_ability_ctx.get('timeout_job_name'))
        try: await query.edit_message_text("Cracking the safe...", reply_markup=None, parse_mode=ParseMode.HTML)
        except TelegramError: pass
        await execute_safecracker_exchange(game, context, user.id, safe_idx, hand_idx)
        return

    # --- Police Patrol Step 1: Selecting the Target Player ---
    elif data.startswith("ability_police_player_select_target_"):
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "Police Patrol" or active_ability_ctx.get('step') != 'police_select_target_player':
            await query.answer("Not a valid Police Patrol action.", show_alert=True)
            return

        # --- Standardized ID Parsing Logic ---
        prefix = "ability_police_player_select_target_"
        target_id_str = data[len(prefix):]
        target_id: Union[int, str] = int(target_id_str) if target_id_str.isdigit() else target_id_str
        # --- End of Standardized Logic ---

        target_player_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, target_id)
        if not target_player_obj or not target_player_obj.get('hand'):
            await query.edit_message_text("Target has no cards. Police stand down.", reply_markup=None)
            game['active_ability_context'] = None
            await advance_turn_or_continue_sequence(game, context)
            return

        active_ability_ctx['targets_chosen'] = [target_id]
        active_ability_ctx['step'] = 'police_select_target_card'
        
        kbd = keyboards.get_police_patrol_target_card_keyboard(
            target_player_hand_count=len(target_player_obj['hand']),
            police_player_id=user.id
        )
        try:
            await query.edit_message_text(f"Targeting {get_player_mention(target_player_obj)}. Choose one of their facedown cards to block:",
                                          reply_markup=kbd, parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.error(f"Err edit Police card choice: {e}")
        return

    # --- Police Patrol Step 2: Selecting the Card to Block ---
    
    elif data.startswith("ability_police_chose_card_"):
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "Police Patrol" or active_ability_ctx.get('step') != 'police_select_target_card':
            await query.answer("Not a valid Police card selection step.", show_alert=True)
            return
            
        try:
            parts = data.split("_")
            police_player_id_from_cb = int(parts[-1])
            target_card_idx = int(parts[-2])
            if user.id != police_player_id_from_cb:
                await query.answer("Button not for you.", True); return
        except (ValueError, IndexError):
            await query.answer("Error processing selection.", show_alert=True)
            return

        target_player_id_from_ctx = active_ability_ctx.get('targets_chosen', [None])[0]
        if not target_player_id_from_ctx:
            await query.answer("Error: Target not found in context.", show_alert=True)
            active_ability_ctx = None # Clear broken context
            await advance_turn_or_continue_sequence(game, context)
            return
            
        active_ability_ctx['cards_selected_indices'] = [target_card_idx]
        active_ability_ctx['step'] = 'police_confirmed_block_target'
        
        original_context_signature = id(active_ability_ctx)
        police_ctx_snapshot = copy.deepcopy(active_ability_ctx)
        
        killer_initiated = await check_for_killer_reaction(
            game_chat_id_for_logic, context, user.id, target_player_id_from_ctx, "Police Patrol", original_context_signature
        )

        if killer_initiated:
            target_player_mention = get_player_mention(game_state_manager.get_player_by_id(game_chat_id_for_logic, target_player_id_from_ctx))
            try:
                await query.edit_message_text(f"Attempting to block Card #{target_card_idx+1} of {target_player_mention}. Waiting for their reaction...", reply_markup=None, parse_mode=ParseMode.HTML)
            except TelegramError: pass
        else:
            cancel_job(context, police_ctx_snapshot.get('timeout_job_name'))
            try:
                await query.edit_message_text("Police Patrol is placing a block...", reply_markup=None, parse_mode=ParseMode.HTML)
            except TelegramError: pass

            game['active_ability_context'] = police_ctx_snapshot
            await execute_police_patrol_ability(game, context, user.id, target_player_id_from_ctx, target_card_idx)
        return

    elif data.startswith("killer_activate_"):
        parts = data.split("_"); player_id_from_cb = int(parts[-2]); killer_ctx_sig_from_cb = int(parts[-1])
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb or \
           active_ability_ctx.get('card_name') != "The Killer" or active_ability_ctx.get('step') != 'killer_prompt_for_use' or \
           id(active_ability_ctx) != killer_ctx_sig_from_cb: # Validate signature
            await query.answer("Not valid Killer action for you now / stale prompt.",True); return
        active_ability_ctx['step'] = 'killer_select_killer_card'
        player_data = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        blocked_indices = set(game.get('blocked_cards', {}).get(user.id, {}).keys())
        try:
            await query.edit_message_text("You chose to use The Killer! Select 'The Killer' card from your hand (facedown choice):",
                reply_markup=keyboards.get_card_selection_keyboard(
                    "killer_chose_card", player_data['hand'], user.id, True, 1, 1, None, False, None, blocked_indices )) # No cancel for this specific step
        except TelegramError as e: logger.error(f"Err edit Killer card choice: {e}")

    elif data.startswith("killer_chose_card_select_"):
        parts = data.split("_"); player_id_from_cb = int(parts[-1]); card_idx_chosen = int(parts[-2])
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb or \
           active_ability_ctx.get('card_name') != "The Killer" or active_ability_ctx.get('step') != 'killer_select_killer_card':
            await query.answer("Not valid Killer card choice step.",True); return
        player_data = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        if not (0 <= card_idx_chosen < len(player_data['hand'])): await query.answer("Invalid card.",True); return
        chosen_card_obj = player_data['hand'][card_idx_chosen]
        cancel_job(context, active_ability_ctx.get('timeout_job_name'))
        try: await query.edit_message_reply_markup(reply_markup=None) 
        except TelegramError: pass
        original_ability_snapshot = active_ability_ctx.get('original_ability_context_snapshot')
        if not original_ability_snapshot: logger.critical("Killer logic error: original_ability_snapshot missing!"); game['active_ability_context'] = None; await advance_turn_or_continue_sequence(game, context); return        
        countered_name = original_ability_snapshot.get('card_name', 'ability')
        original_user_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, original_ability_snapshot.get('player_id'))

        if chosen_card_obj.get('name') == "The Killer":
            player_data['hand'].pop(card_idx_chosen); game['discard_pile'].append(chosen_card_obj)
            await send_message_to_player(context, user.id, f"Bang! You used The Killer, countering {escape_html(countered_name)} from {get_player_mention(original_user_obj)}!", parse_mode=ParseMode.HTML)
            await context.bot.send_message(game_chat_id_for_logic, f"💥 {get_player_mention(player_data)} used <b>The Killer</b> to shut down {get_player_mention(original_user_obj)}'s <b>{escape_html(countered_name)}</b>!", parse_mode=ParseMode.HTML)
            logger.info(f"Killer success by {user.id}. Original ability '{countered_name}' by {original_ability_snapshot.get('player_id')} nullified.")
            game['active_ability_context'] = None 
            game['current_player_id'] = original_ability_snapshot.get('player_id') 
            await advance_turn_or_continue_sequence(game, context)
        else: # Failed Killer
            penalty_msg_p = f"Oops! You discarded '{escape_html(chosen_card_obj.get('name'))}', not The Killer. The ability was not countered."
            penalty_msg_g = f"Uh oh! {get_player_mention(player_data)} fumbled their Killer attempt by discarding '{escape_html(chosen_card_obj.get('name'))}'!"

            if game.get('deck'):
                pen_card = game['deck'].pop()
                player_data['hand'].append(pen_card)
                penalty_msg_p += "\nYou also receive a penalty card from the deck."
                penalty_msg_g += " They receive a penalty card."
                logger.info(f"Killer Fail: Player {user.id} drew penalty card '{pen_card.get('name')}' (hidden).")
            else:
                penalty_msg_p += "\n(The deck was empty, so you got lucky... no card drawn.)"
                penalty_msg_g += " (The deck was empty, so no penalty card was drawn.)"

            await send_message_to_player(context, user.id, penalty_msg_p, parse_mode=ParseMode.HTML)
            await context.bot.send_message(game_chat_id_for_logic, penalty_msg_g + f" Original ability ({escape_html(countered_name)}) by {get_player_mention(original_user_obj)} proceeds!", parse_mode=ParseMode.HTML)
            game['active_ability_context'] = None 
            await resume_original_ability_after_killer_interaction(game, context, original_ability_snapshot, "failed")
            return

    elif data.startswith("killer_decline_"):
        parts = data.split("_"); player_id_from_cb = int(parts[-2]); killer_ctx_sig_from_cb = int(parts[-1])
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb or \
           active_ability_ctx.get('card_name') != "The Killer" or active_ability_ctx.get('step') != 'killer_prompt_for_use' or \
           id(active_ability_ctx) != killer_ctx_sig_from_cb:
            await query.answer("Not a valid Killer decline / stale prompt.",True); return

        cancel_job(context, active_ability_ctx.get('timeout_job_name'))
        try: 
            await query.edit_message_text("You chose not to use The Killer.", reply_markup=None)
        except TelegramError: pass
        
        original_ability_snapshot = active_ability_ctx.get('original_ability_context_snapshot')
        if not original_ability_snapshot: 
            logger.critical("Killer decline error: original_ability_snapshot missing!")
            game['active_ability_context'] = None
            await advance_turn_or_continue_sequence(game, context)
            return

        countered_name = original_ability_snapshot.get('card_name', 'ability')
        original_user_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, original_ability_snapshot.get('player_id'))
        
        # --- THIS IS THE FIX ---
        # Get the correct player dictionary for the user who declined
        declining_player_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        
        await context.bot.send_message(
            game_chat_id_for_logic, 
            f"{get_player_mention(declining_player_obj)} declined to use Killer. Original ability ({escape_html(countered_name)}) by {get_player_mention(original_user_obj)} proceeds!", 
            parse_mode=ParseMode.HTML
        )
        # --- END OF FIX ---

        game['active_ability_context'] = None
        await resume_original_ability_after_killer_interaction(game, context, original_ability_snapshot, "declined")
        return # Added return for safety    

    elif data.startswith("ability_") and data.endswith("_overall_cancel"):
        player_id_from_cb_str = data.split("_")[-1]
        try: player_id_from_cb = int(player_id_from_cb_str)
        except ValueError: await query.answer("Invalid cancel format.", True); return

        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb :
            await query.answer("Not your active ability to cancel.", True); return
        
        ability_name_in_ctx = active_ability_ctx.get('card_name', 'Unknown Ability')
        logger.info(f"Player {user.id} cancelled ability {ability_name_in_ctx} (CBQ: {data}). Step: {active_ability_ctx.get('step')}")
        cancel_job(context, active_ability_ctx.get('timeout_job_name'))
        game['active_ability_context'] = None
        try: 
            msg_text_edited = f"You cancelled using {escape_html(ability_name_in_ctx)}."
            target_msg = query.message
            if target_msg.text : await query.edit_message_text(msg_text_edited, reply_markup=None, parse_mode=ParseMode.HTML)
            elif target_msg.caption : await query.edit_message_caption(caption=msg_text_edited, reply_markup=None, parse_mode=ParseMode.HTML)
            else: await send_message_to_player(context, user.id, msg_text_edited) # Fallback to new message
        except TelegramError: pass
        player_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
        await context.bot.send_message(game_chat_id_for_logic, f"{get_player_mention(player_obj)} cancelled their {escape_html(ability_name_in_ctx)} action.", parse_mode=ParseMode.HTML)
        await advance_turn_or_continue_sequence(game, context)
        return

    elif data.startswith("ability_gangster_type_own_") or data.startswith("ability_gangster_type_other_"):
        player_id_from_cb = int(data.split("_")[-1])
        if not active_ability_ctx or active_ability_ctx.get('player_id') != user.id or user.id != player_id_from_cb or \
           active_ability_ctx.get('card_name') != "The Gangster" or active_ability_ctx.get('step') != 'gangster_select_action_type':
            await query.answer("Not a valid Gangster action.", show_alert=True); return

        if data.startswith("ability_gangster_type_own_"):
            active_ability_ctx['gangster_swap_type'] = 'own_vs_other'
            active_ability_ctx['step'] = 'gangster_own_select_own_card'
            player_data = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)
            
            # --- THIS IS THE FIX ---
            # Explicitly get blocked indices and pass them using the keyword argument
            blocked_indices = set(game.get('blocked_cards', {}).get(str(user.id), {}).keys())
            kbd = keyboards.get_card_selection_keyboard(
                purpose_prefix="ability_gangster_own_chose_own_card",
                player_hand=player_data['hand'],
                player_id_context=user.id,
                facedown=True, num_to_select=1, min_to_select=1,
                allow_cancel=True,
                cancel_callback_data=f"ability_gangster_overall_cancel_{user.id}",
                blocked_card_indices=blocked_indices # Use the keyword argument for clarity
            )
            # --- END OF FIX ---

            await query.edit_message_text("Gangster (Own vs Other): Choose YOUR card to swap (facedown):", reply_markup=kbd)
            return

        elif data.startswith("ability_gangster_type_other_"):
            active_ability_ctx['gangster_swap_type'] = 'other_vs_other'
            active_ability_ctx['step'] = 'gangster_others_select_opp1'
            kbd = keyboards.get_target_player_keyboard(
                game, user.id, "ability_gangster_others_chose_opp1", 1, 1, None, None, # exclude_ids=[user.id] is implicit
                f"ability_gangster_overall_cancel_{user.id}"
            )
            await query.edit_message_text("Gangster (Other vs Other): Choose Opponent 1:", reply_markup=kbd)
            return

    elif data.startswith("ability_gangster_own_chose_own_card_select_"):
        if not active_ability_ctx or \
           active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Gangster" or \
           active_ability_ctx.get('step') != 'gangster_own_select_own_card':
            await query.answer("Not a valid Gangster action (Own card selection).", show_alert=True)
            return

        try:
            parts = data.split("_")
            gangster_id_from_cb = int(parts[-1])
            selected_own_card_idx = int(parts[-2])
            if user.id != gangster_id_from_cb:
                await query.answer("Button not for you.", True); return
        except (ValueError, IndexError):
            await query.answer("Error processing your card selection.", show_alert=True)
            return

        active_ability_ctx['p1_id'] = user.id
        active_ability_ctx['p1_card_idx'] = selected_own_card_idx
        active_ability_ctx['step'] = 'gangster_own_select_opponent'

        kbd = keyboards.get_target_player_keyboard(
            game, user.id, "ability_gangster_own_chose_opponent", 1, 1,
            cancel_callback_data=f"ability_gangster_overall_cancel_{user.id}"
        )
        await query.edit_message_text(
            f"Gangster (Own vs Other): You selected your Card #{selected_own_card_idx+1}.\nNow, choose an Opponent to swap with:",
            reply_markup=kbd, parse_mode=ParseMode.HTML
        )
        return

    elif data.startswith("ability_gangster_own_chose_opponent_select_target_"):
        if not active_ability_ctx or \
           active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Gangster" or \
           active_ability_ctx.get('step') != 'gangster_own_select_opponent':
            await query.answer("Not a valid Gangster action (Opponent selection).", show_alert=True)
            return

        # --- Standardized ID Parsing Logic ---
        prefix = "ability_gangster_own_chose_opponent_select_target_"
        target_id_str = data[len(prefix):]
        target_opp_id: Union[int, str] = int(target_id_str) if target_id_str.isdigit() else target_id_str
        # --- End of Standardized Logic ---

        opponent_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, target_opp_id)
        if not opponent_obj or not opponent_obj.get('hand'):
            await query.answer("Selected Opponent is invalid or has no cards.", show_alert=True); return

        active_ability_ctx['p2_id'] = target_opp_id
        active_ability_ctx['step'] = 'gangster_own_select_opponent_card'
        
        blocked_indices_opp = set(game.get('blocked_cards', {}).get(str(target_opp_id), {}).keys())
        kbd = keyboards.get_card_selection_keyboard(
            purpose_prefix="g_own_oppcard", player_hand=opponent_obj['hand'], player_id_context=user.id,
            facedown=True, num_to_select=1, min_to_select=1,
            allow_cancel=True, cancel_callback_data=f"ability_gangster_overall_cancel_{user.id}",
            blocked_card_indices=blocked_indices_opp
        )
        await query.edit_message_text(
            f"Gangster (Own vs Other): Now, select one of {get_player_mention(opponent_obj)}'s facedown cards to take:",
            reply_markup=kbd, parse_mode=ParseMode.HTML
        )
        return

    # ... (inside handle_callback_query)

    elif data.startswith("g_own_oppcard_select_"): # Handling the new shortened prefix
        # Expected format: "g_own_oppcard_select_{card_idx}_{gangster_player_id}"
        
        if not active_ability_ctx or \
        active_ability_ctx.get('player_id') != user.id or \
        active_ability_ctx.get('card_name') != "The Gangster" or \
        active_ability_ctx.get('step') != 'gangster_own_select_opponent_card' or \
        active_ability_ctx.get('gangster_swap_type') != 'own_vs_other' or \
        active_ability_ctx.get('p1_id') != user.id or active_ability_ctx.get('p1_card_idx') is None or \
        active_ability_ctx.get('p2_id') is None: # Ensure previous steps are done
            await query.answer("Not a valid Gangster action (Opponent card selection for own swap).", show_alert=True)
            return

        try:
            parts = data.split("_")
            # parts: ["g", "own", "oppcard", "select", "{card_idx}", "{gangster_player_id}"]
            gangster_id_from_cb = int(parts[-1])
            selected_opp_card_idx = int(parts[-2])

            if user.id != gangster_id_from_cb: # Validate gangster ID from callback
                logger.warning(f"Gangster Own Opp Card Select (short prefix): Mismatch. CB gangster: {gangster_id_from_cb} vs user {user.id}. Data: {data}")
                await query.answer("Context mismatch (player ID).", show_alert=True)
                return

            # Retrieve opponent's ID (p2_id) from the context
            opp_id_from_ctx = active_ability_ctx.get('p2_id')
            if not opp_id_from_ctx:
                logger.error(f"Gangster Own Opp Card Select (short prefix): Opponent ID (p2_id) missing from context. Data: {data}")
                await query.answer("Context error: Opponent ID missing.", show_alert=True)
                return

        except (ValueError, IndexError) as e:
            logger.error(f"Gangster (Own v Other) Opponent Card Select (short prefix): Error parsing CBQ '{data}': {e}", exc_info=True)
            await query.answer("Error processing opponent's card selection.", show_alert=True)
            return

        opponent_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, opp_id_from_ctx)
        if not opponent_obj or not (0 <= selected_opp_card_idx < len(opponent_obj.get('hand', []))):
            await query.answer("Invalid card selection for the opponent.", show_alert=True)
            return

        active_ability_ctx['p2_card_idx'] = selected_opp_card_idx
        active_ability_ctx['step'] = 'gangster_own_confirm_swap' # Next step: Final Confirmation

        # gangster_player_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id) # Not needed for display text

        confirm_text = (
            f"<b>Gangster (Own vs Other) - Final Confirmation:</b>\n\n"
            f"You are about to swap:\n"
            f"- Your Card #{active_ability_ctx['p1_card_idx']+1} (facedown)\n"
            f"WITH\n"
            f"- Card #{active_ability_ctx['p2_card_idx']+1} from {get_player_mention(opponent_obj)} (facedown)\n\n"
            f"Proceed with the swap?"
        )
        
        confirm_cb_data = f"ability_gangster_own_final_confirm_{user.id}" 
        cancel_cb_data = f"ability_gangster_overall_cancel_{user.id}"
        kbd = keyboards.get_confirmation_keyboard(confirm_cb_data, cancel_cb_data)
        
        try:
            await query.edit_message_text(confirm_text, reply_markup=kbd, parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.error(f"Error editing Gangster (Own) final confirmation message (short prefix path): {e}")
        return

    elif data.startswith("ability_gangster_own_chose_opponent_card_"):       
        if not active_ability_ctx or \
           active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Gangster" or \
           active_ability_ctx.get('step') != 'gangster_own_select_opponent_card' or \
           active_ability_ctx.get('gangster_swap_type') != 'own_vs_other' or \
           active_ability_ctx.get('p1_id') != user.id or active_ability_ctx.get('p1_card_idx') is None or \
           active_ability_ctx.get('p2_id') is None: # Ensure previous steps are done
            await query.answer("Not a valid Gangster action (Opponent card selection for own swap).", show_alert=True)
            return

        try:
            parts = data.split("_")
            # Callback: ability_gangster_own_chose_opponent_card_{opp_id}_select_{card_idx}_{gangster_player_id}
            
            gangster_id_from_cb = int(parts[-1])
            selected_opp_card_idx = int(parts[-2])
            
            opp_id_str_parts = []
            start_index_opp_id = -1
            end_index_opp_id = -1

            for i, part in enumerate(parts):
                if part == "card" and parts[i-1] == "opponent" and start_index_opp_id == -1:
                    start_index_opp_id = i + 1
                if part == "select" and parts[i+1] == str(selected_opp_card_idx) and end_index_opp_id == -1:
                    end_index_opp_id = i
                    break
            
            if start_index_opp_id == -1 or end_index_opp_id == -1 or start_index_opp_id >= end_index_opp_id:
                 raise ValueError("Could not accurately parse Opponent ID from callback.")

            opp_id_reconstructed_str = "_".join(parts[start_index_opp_id:end_index_opp_id])
            
            opp_id_from_cb: Union[int, str]
            if opp_id_reconstructed_str.startswith("ai_") and opp_id_reconstructed_str.count('_') >= 2:
                opp_id_from_cb = opp_id_reconstructed_str
            elif opp_id_reconstructed_str.isdigit():
                opp_id_from_cb = int(opp_id_reconstructed_str)
            else:
                raise ValueError(f"Invalid Opponent ID format parsed: {opp_id_reconstructed_str}")

            if user.id != gangster_id_from_cb or active_ability_ctx.get('p2_id') != opp_id_from_cb:
                logger.warning(f"Gangster Own Opp Card Select: Mismatch. CB gangster: {gangster_id_from_cb} vs user {user.id}. CB opp: {opp_id_from_cb} vs ctx p2_id: {active_ability_ctx.get('p2_id')}. Data: {data}")
                await query.answer("Context mismatch during opponent's card selection.", show_alert=True)
                return

        except (ValueError, IndexError) as e:
            logger.error(f"Gangster (Own v Other) Opponent Card Select: Error parsing CBQ '{data}': {e}", exc_info=True)
            await query.answer("Error processing opponent's card selection.", show_alert=True)
            return

        opponent_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, opp_id_from_cb)
        if not opponent_obj or not (0 <= selected_opp_card_idx < len(opponent_obj.get('hand', []))):
            await query.answer("Invalid card selection for the opponent.", show_alert=True)
            return

        active_ability_ctx['p2_card_idx'] = selected_opp_card_idx
        active_ability_ctx['step'] = 'gangster_own_confirm_swap' # Next step: Final Confirmation

        gangster_player_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, user.id)

        confirm_text = (
            f"<b>Gangster (Own vs Other) - Final Confirmation:</b>\n\n"
            f"You are about to swap:\n"
            f"- Your Card #{active_ability_ctx['p1_card_idx']+1} (facedown)\n"
            f"WITH\n"
            f"- Card #{active_ability_ctx['p2_card_idx']+1} from {get_player_mention(opponent_obj)} (facedown)\n\n"
            f"Proceed with the swap?"
        )
        
        confirm_cb_data = f"ability_gangster_own_final_confirm_{user.id}" # Gangster player ID for confirm
        cancel_cb_data = f"ability_gangster_overall_cancel_{user.id}"

        kbd = keyboards.get_confirmation_keyboard(confirm_cb_data, cancel_cb_data)
        
        try:
            await query.edit_message_text(confirm_text, reply_markup=kbd, parse_mode=ParseMode.HTML)
        except TelegramError as e:
            logger.error(f"Error editing Gangster (Own) final confirmation message: {e}")
        return

        # ... (inside handle_callback_query)

        # This block might be new or need to replace a faulty confirmation step.

    elif data.startswith("ability_gangster_own_final_confirm_"):        
        if not active_ability_ctx or \
           active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Gangster" or \
           active_ability_ctx.get('step') != 'gangster_own_confirm_swap':
            await query.answer("Not a valid Gangster confirmation.", show_alert=True)
            return

        # --- Standardized ID Parsing for the Confirmer (Gangster) ---
        prefix = "ability_gangster_own_final_confirm_"
        player_id_str = data[len(prefix):]
        player_id_from_cb: Union[int, str] = int(player_id_str) if player_id_str.isdigit() else player_id_str
        if user.id != player_id_from_cb:
            await query.answer("Not your confirmation.", True); return
        # --- End of Standardized Logic ---

        # All necessary data should already be in the context from previous steps.
        p1_id = active_ability_ctx.get('p1_id')
        p1_card_idx = active_ability_ctx.get('p1_card_idx')
        p2_id = active_ability_ctx.get('p2_id')
        p2_card_idx = active_ability_ctx.get('p2_card_idx')

        if not all([p1_id == user.id, p1_card_idx is not None, p2_id is not None, p2_card_idx is not None]):
            await query.answer("Error: Swap details are incomplete.", show_alert=True)
            return

        original_context_signature = id(active_ability_ctx)
        gangster_ctx_snapshot = copy.deepcopy(active_ability_ctx)
        
        # The gangster is targeting the opponent (p2)
        killer_initiated = await check_for_killer_reaction(
            game_chat_id_for_logic, context, user.id, p2_id, "The Gangster (Swap)", original_context_signature
        )

        if killer_initiated:
            opponent_mention = get_player_mention(game_state_manager.get_player_by_id(game_chat_id_for_logic, p2_id))
            await query.edit_message_text(
                f"Attempting swap with {opponent_mention}. Waiting for their reaction...", 
                reply_markup=None, parse_mode=ParseMode.HTML
            )
        else:
            cancel_job(context, gangster_ctx_snapshot.get('timeout_job_name'))
            await query.edit_message_text("The Gangster is making a personal exchange...", reply_markup=None, parse_mode=ParseMode.HTML)

            game['active_ability_context'] = gangster_ctx_snapshot
            swap_details = { 'type': 'own_vs_other', 'p1_id': p1_id, 'p1_card_idx': p1_card_idx, 'p2_id': p2_id, 'p2_card_idx': p2_card_idx }
            await execute_gangster_swap(game, context, user.id, swap_details)
        return

    elif data.startswith("ability_gangster_others_chose_opp1_select_target_"):
        if not active_ability_ctx or \
           active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Gangster" or \
           active_ability_ctx.get('step') != 'gangster_others_select_opp1':
            await query.answer("Not a valid Gangster action (Opponent 1 selection).", show_alert=True)
            return

        # --- Standardized ID Parsing Logic ---
        prefix = "ability_gangster_others_chose_opp1_select_target_"
        target_id_str = data[len(prefix):]
        target_opp1_id: Union[int, str] = int(target_id_str) if target_id_str.isdigit() else target_id_str
        # --- End of Standardized Logic ---

        opponent1_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, target_opp1_id)
        if not opponent1_obj or not opponent1_obj.get('hand'):
            await query.answer("Selected Opponent 1 is invalid or has no cards.", show_alert=True)
            return

        active_ability_ctx['p1_id'] = target_opp1_id
        active_ability_ctx['step'] = 'gangster_others_select_opp1_card'
        
        blocked_indices_opp1 = set(game.get('blocked_cards', {}).get(str(target_opp1_id), {}).keys())
        kbd = keyboards.get_card_selection_keyboard(
            "g_oth_opp1card", opponent1_obj['hand'], user.id, True, 1, 1, None,
            True, f"ability_gangster_overall_cancel_{user.id}", blocked_indices_opp1
        )
        await query.edit_message_text(
            f"Gangster (Other vs Other): You chose {get_player_mention(opponent1_obj)} as Opponent 1.\nNow, select one of THEIR facedown cards:",
            reply_markup=kbd, parse_mode=ParseMode.HTML
        )
        return

    elif data.startswith("g_oth_opp1card_select_"): 
        if not active_ability_ctx or \
           active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Gangster" or \
           active_ability_ctx.get('step') != 'gangster_others_select_opp1_card' or \
           active_ability_ctx.get('gangster_swap_type') != 'other_vs_other':
            await query.answer("Not a valid Gangster action (Opponent 1 card selection).", show_alert=True)
            return

        try:
            parts = data.split("_")
            gangster_id_from_cb = int(parts[-1])
            selected_card_idx = int(parts[-2])

            if user.id != gangster_id_from_cb:
                logger.warning(f"Gangster Opp1 Card Select (short prefix): Mismatch. CB gangster: {gangster_id_from_cb} vs user {user.id}. Data: {data}")
                await query.answer("Context mismatch (player ID).", show_alert=True)
                return

            opp1_id_from_ctx = active_ability_ctx.get('p1_id')
            if not opp1_id_from_ctx:
                logger.error(f"Gangster Opp1 Card Select (short prefix): Opponent 1 ID (p1_id) missing from context. Data: {data}")
                await query.answer("Context error: Opponent 1 ID missing.", show_alert=True)
                return
        
        except (ValueError, IndexError) as e:
            logger.error(f"Gangster (Other v Other) Opp1 Card Select (short prefix): Error parsing CBQ '{data}': {e}", exc_info=True)
            await query.answer("Error processing Opponent 1's card selection.", show_alert=True)
            return

        opponent1_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, opp1_id_from_ctx)
        if not opponent1_obj or not (0 <= selected_card_idx < len(opponent1_obj.get('hand', []))):
            await query.answer("Invalid card selection for Opponent 1.", show_alert=True)
            return

        active_ability_ctx['p1_card_idx'] = selected_card_idx
        active_ability_ctx['step'] = 'gangster_others_select_opp2' 

        ids_to_exclude_for_opp2 = [user.id, opp1_id_from_ctx] # Exclude Gangster and Opponent 1

        kbd = keyboards.get_target_player_keyboard(
            game, user.id, 
            ability_context_key_prefix="ability_gangster_others_chose_opp2", 
            max_targets=1, min_targets=1,
            exclude_ids=ids_to_exclude_for_opp2,
            cancel_callback_data=f"ability_gangster_overall_cancel_{user.id}"
        )
        
        if not any(btn_row for btn_row in kbd.inline_keyboard if any(b.callback_data != f"ability_gangster_overall_cancel_{user.id}" and not b.callback_data.startswith("no_action") for b in btn_row)):
            await query.edit_message_text(
                 f"Gangster (Other vs Other): You selected Card #{selected_card_idx+1} from {get_player_mention(opponent1_obj)}.\n"
                 "However, there are no other valid opponents to select for Opponent 2. Ability cancelled.",
                 reply_markup=None, parse_mode=ParseMode.HTML
            )
            game['active_ability_context'] = None
            await advance_turn_or_continue_sequence(game, context)
            return

        await query.edit_message_text(
            f"Gangster (Other vs Other): You selected Card #{selected_card_idx+1} from {get_player_mention(opponent1_obj)}.\n"
            f"Now, choose Opponent 2 (cannot be {get_player_mention(opponent1_obj)} or yourself):",
            reply_markup=kbd,
            parse_mode=ParseMode.HTML
        )
        return
    
    # ADD THIS ENTIRE BLOCK to simple_bot.py inside handle_callback_query()

    elif data.startswith("ability_gangster_others_chose_opp2_select_target_"):
        if not active_ability_ctx or \
           active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Gangster" or \
           active_ability_ctx.get('step') != 'gangster_others_select_opp2':
            await query.answer("Not a valid Gangster action (Opponent 2 selection).", show_alert=True)
            return

        # --- Standardized ID Parsing Logic ---
        prefix = "ability_gangster_others_chose_opp2_select_target_"
        target_id_str = data[len(prefix):]
        target_opp2_id: Union[int, str] = int(target_id_str) if target_id_str.isdigit() else target_id_str
        # --- End of Standardized Logic ---

        opponent2_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, target_opp2_id)
        if not opponent2_obj or not opponent2_obj.get('hand'):
            await query.answer("Selected Opponent 2 is invalid or has no cards.", show_alert=True); return

        active_ability_ctx['p2_id'] = target_opp2_id
        active_ability_ctx['step'] = 'gangster_others_select_opp2_card'

        opponent1_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, active_ability_ctx['p1_id'])
        blocked_indices_opp2 = set(game.get('blocked_cards', {}).get(str(target_opp2_id), {}).keys())

        kbd = keyboards.get_card_selection_keyboard(
            "g_oth_opp2card", opponent2_obj['hand'], user.id, True, 1, 1, None,
            True, f"ability_gangster_overall_cancel_{user.id}", blocked_indices_opp2
        )
        await query.edit_message_text(
            f"Gangster (Other vs Other):\n- Opponent 1: {get_player_mention(opponent1_obj)}, Card #{active_ability_ctx['p1_card_idx']+1}\n- Opponent 2: {get_player_mention(opponent2_obj)}\n\nNow, select {get_player_mention(opponent2_obj)}'s card:",
            reply_markup=kbd, parse_mode=ParseMode.HTML)
        return

    elif data.startswith("g_oth_opp2card_select_"):
        if not active_ability_ctx or \
           active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Gangster" or \
           active_ability_ctx.get('step') != 'gangster_others_select_opp2_card':
            await query.answer("Not a valid Gangster action.", show_alert=True)
            return
            
        parts = data.split("_"); gangster_id_from_cb = int(parts[-1]); selected_card_idx = int(parts[-2])
        if user.id != gangster_id_from_cb:
            await query.answer("Context mismatch.", show_alert=True); return

        opp2_id_from_ctx = active_ability_ctx.get('p2_id')
        opponent2_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, opp2_id_from_ctx)
        if not opponent2_obj or not (0 <= selected_card_idx < len(opponent2_obj.get('hand', []))):
            await query.answer("Invalid card selection for Opponent 2.", show_alert=True); return

        active_ability_ctx['p2_card_idx'] = selected_card_idx
        active_ability_ctx['step'] = 'gangster_others_confirm_swap'
        opponent1_obj = game_state_manager.get_player_by_id(game_chat_id_for_logic, active_ability_ctx['p1_id'])

        confirm_text = (f"<b>Gangster (Other vs Other) - Final Confirmation:</b>\n\n"
                        f"You are about to swap:\n- Card #{active_ability_ctx['p1_card_idx']+1} from {get_player_mention(opponent1_obj)}\n"
                        f"WITH\n- Card #{active_ability_ctx['p2_card_idx']+1} from {get_player_mention(opponent2_obj)}\n\n"
                        f"Proceed?")
        kbd = keyboards.get_confirmation_keyboard(f"ability_gangster_others_final_confirm_{user.id}", f"ability_gangster_overall_cancel_{user.id}")
        await query.edit_message_text(confirm_text, reply_markup=kbd, parse_mode=ParseMode.HTML)
        return

    elif data.startswith("ability_gangster_others_final_confirm_"):
        if not active_ability_ctx or \
           active_ability_ctx.get('player_id') != user.id or \
           active_ability_ctx.get('card_name') != "The Gangster" or \
           active_ability_ctx.get('step') != 'gangster_others_confirm_swap':
            await query.answer("Not a valid Gangster confirmation.", show_alert=True)
            return
            
        # --- Standardized ID Parsing for Confirmer (Gangster) ---
        prefix = "ability_gangster_others_final_confirm_"
        player_id_str = data[len(prefix):]
        player_id_from_cb: Union[int, str] = int(player_id_str) if player_id_str.isdigit() else player_id_str
        if user.id != player_id_from_cb:
            await query.answer("Not your confirmation.", True); return
        # --- End of Standardized Logic ---

        # The Killer is not checked here, as the Gangster is not the target.
        cancel_job(context, active_ability_ctx.get('timeout_job_name'))
        try: 
            await query.edit_message_text("The Gangster is making a deal...", reply_markup=None, parse_mode=ParseMode.HTML)
        except TelegramError: pass

        swap_details = {
            'type': 'other_vs_other',
            'p1_id': active_ability_ctx['p1_id'], 'p1_card_idx': active_ability_ctx['p1_card_idx'],
            'p2_id': active_ability_ctx['p2_id'], 'p2_card_idx': active_ability_ctx['p2_card_idx']
        }
        await execute_gangster_swap(game, context, user.id, swap_details)
        return

    # Handle non-action callbacks
    elif data.startswith("no_action_dummy"): await query.answer("This option is not currently available.", show_alert=True); return
    elif data.startswith("no_action_"): 
        if "max_limit" in data: await query.answer(f"Max {INITIAL_CARDS_TO_VIEW} cards viewed.",True)
        elif "viewed" in data : await query.answer("Already viewed this card.",True)
        elif "card_blocked" in data : await query.answer("This card is blocked by Police Patrol!",True)
        elif "empty_slot" in data: await query.answer("This card slot is empty.",True)
        elif "max_selected" in data: await query.answer("Maximum cards already selected.",True)
        else: await query.answer("Action unavailable/done.",True)
        return
    
    elif data.startswith("play_again_"): 
        if data == "play_again_new_game": 
            pass 
        else:
            try:
                await query.edit_message_reply_markup(reply_markup=None) 
                await query.answer("Option not implemented yet.", show_alert=True)
            except TelegramError: pass
        return

    else: 
        if game:
            logger.warning(f"Unhandled CBQ (end of func with game): {data} Ph:{game.get('phase')} U:{user.id}")
        else:
            logger.warning(f"Unhandled CBQ (end of func NO game): {data} U:{user.id}")
        pass

async def set_bot_commands(application: Application):
    commands = [
        BotCommand("start", "Start/restart bot and see main menu."),
        BotCommand("newgame", "Start a new game of Omerta in this chat."),
        BotCommand("rules", "Learn the detailed rules of Omerta."),
        BotCommand("help", "Get help with commands and basic gameplay."),
        BotCommand("endgame", "End the current Omerta game in this chat (if any).")
    ]
    try: 
        await application.bot.set_my_commands(commands)
        logger.info("Bot commands set successfully.")
    except Exception as e:
        logger.error(f"Failed to set bot commands: {e}")

def main() -> None:
    if not BOT_TOKEN: 
        logger.critical("FATAL: BOT_TOKEN not found. Bot cannot start.")
        return

    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("rules", rules_command))
    application.add_handler(CommandHandler("leaderboard", leaderboard_command))
    application.add_handler(CommandHandler("newgame", new_game_command_entry))
    application.add_handler(CommandHandler("endgame", endgame_command))
    
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^🏠 Main Menu$"), start_command))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^🎮 New Game$"), new_game_command_entry))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^🛑 End Game$"), endgame_command))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^🏆 Leaderboard$"), leaderboard_command))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^📜 Rules$"), rules_command))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^❓ Help$"), help_command))

    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_error_handler(custom_error_handler)
    application.post_init = set_bot_commands
    
    logger.info("Omerta Bot starting polling...")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.critical(f"Bot polling failed critically: {e}", exc_info=True)
    finally:
        logger.info("Omerta Bot has shut down.")

if __name__ == "__main__":
    main()