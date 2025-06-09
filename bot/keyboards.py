from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from typing import List, Set, Optional, Dict, Union

def get_main_reply_keyboard() -> ReplyKeyboardMarkup:
    keyboard = [
        ["🏠 Main Menu"],
        ["🎮 New Game", "🛑 End Game"],
        ["🏆 Leaderboard", "📜 Rules", "❓ Help"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_start_keyboard() -> InlineKeyboardMarkup:
    keyboard = [[InlineKeyboardButton("🎮 New Game", callback_data="main_new_game")],
                [InlineKeyboardButton("❓ Help", callback_data="main_help")]]
    return InlineKeyboardMarkup(keyboard)

def get_leaderboard_options_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("🏆 My Personal Stats", callback_data="leaderboard_personal_stats")],
        [InlineKeyboardButton("📊 This Chat's Leaderboard", callback_data="leaderboard_chat_top_5")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_game_mode_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("👤 A Quiet Game (vs AI)", callback_data="mode_select_solo")],
        [InlineKeyboardButton("👥 A Showdown with Rivals", callback_data="mode_select_group")],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu_return")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_add_ai_options_keyboard(current_total_players: int, max_players: int) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    max_ai_can_add = min(6, max_players - current_total_players)

    if max_ai_can_add <= 0:
         buttons.append([InlineKeyboardButton("Cannot Add More AI", callback_data="no_action_dummy_ai_full")])
    else:
        for i in range(1, max_ai_can_add + 1):
            row.append(InlineKeyboardButton(str(i), callback_data=f"ai_add_count_{i}"))
            if len(row) == 3: 
                buttons.append(row)
                row = []
        if row: 
            buttons.append(row)
    
    buttons.append([InlineKeyboardButton("❌ Cancel AI Addition", callback_data="ai_add_cancel")])
    return InlineKeyboardMarkup(buttons)

def get_setup_phase_keyboard(can_start_game: bool, is_solo_mode: bool, current_total_players: int, max_players: int, min_players: int) -> InlineKeyboardMarkup:
    keyboard = []
    if can_start_game: 
        keyboard.append([InlineKeyboardButton("🚀 Start Game", callback_data="setup_start_game")])
    else:
        needed = min_players - current_total_players
        keyboard.append([InlineKeyboardButton(f"🚀 Start Game (Need {needed} more)", callback_data="no_action_dummy_need_more")]) 
    
    if current_total_players < max_players: 
        keyboard.append([InlineKeyboardButton("🤖 Add AI Player(s)", callback_data="setup_ask_add_ai")]) 
    
    if not is_solo_mode and not can_start_game and current_total_players < min_players :
         keyboard.append([InlineKeyboardButton("👤 Switch to Solo & Add AI", callback_data="setup_force_solo")])
    
    return InlineKeyboardMarkup(keyboard)

def get_join_game_keyboard(game: Optional[Dict] = None, max_players_const: Optional[int] = None) -> InlineKeyboardMarkup:
    
    keyboard_buttons = [[InlineKeyboardButton("🚪 Join Game", callback_data="lobby_join_game")]]
    
    if game and game.get('phase') == "joining": 
        num_total_players = len(game.get('players', [])) + len(game.get('ai_players', []))
        
        _MAX_PLAYERS = max_players_const if max_players_const is not None else 9 

        if num_total_players < _MAX_PLAYERS:
             keyboard_buttons.append([InlineKeyboardButton("🤖 Add AI (Host Only)", callback_data="group_lobby_ask_add_ai")])
                                
    return InlineKeyboardMarkup(keyboard_buttons)


def get_al_capone_continue_keyboard() -> InlineKeyboardMarkup:
    keyboard = [[InlineKeyboardButton("➡️ Continue to Game", callback_data="flow_al_capone_continue")]]
    return InlineKeyboardMarkup(keyboard)

def get_card_viewing_keyboard(player_hand: List[Dict], viewed_indices: Set[int], max_can_view: int, hand_size: int = 4) -> InlineKeyboardMarkup:
    buttons_grid: List[List[InlineKeyboardButton]] = []
    
    for i in range(hand_size): 
        text = f"Card #{i+1}"
        callback = f"viewing_select_card_{i}"
        
        if i >= len(player_hand): 
            text = f"Slot #{i+1} (Empty)"
            callback = f"no_action_empty_slot_{i}"
        elif i in viewed_indices:
            text += " ✅" 
            callback = f"no_action_viewed_{i}" 
        elif len(viewed_indices) >= max_can_view:
            callback = f"no_action_max_limit_{i}"
        
       
        if i % 2 == 0:
            buttons_grid.append([InlineKeyboardButton(text, callback_data=callback)])
        else:
            buttons_grid[-1].append(InlineKeyboardButton(text, callback_data=callback))
            
    if len(viewed_indices) >= max_can_view:
         buttons_grid.append([InlineKeyboardButton("👍 Done Viewing", callback_data="viewing_confirm_done")])

    return InlineKeyboardMarkup(buttons_grid)

def get_player_turn_actions_keyboard(game: Dict, current_player_data: Dict, is_first_cycle_al_capone: bool) -> InlineKeyboardMarkup:
    buttons = []
    player_id = current_player_data['id']
    
    if not current_player_data.get('cannot_call_omerta', False) and \
       current_player_data.get('status') != "skipped_turn": 
        buttons.append([InlineKeyboardButton("🗣️ Call Omerta", callback_data=f"turn_call_omerta_{player_id}")])

    buttons.append([InlineKeyboardButton("🃏 Draw from Deck", callback_data=f"turn_draw_deck_{player_id}")])

    if not is_first_cycle_al_capone:
        can_draw_discard = False
        top_discard_name = ""
        if game['discard_pile']:
            top_discard = game['discard_pile'][-1]
            top_discard_name = top_discard.get('name', 'Card')
            if top_discard.get('type') == 'bottle' or \
               (top_discard.get('type') == 'character' and top_discard.get('name') == 'The Alibi'):
                can_draw_discard = True
        
        if can_draw_discard:
            buttons.append([InlineKeyboardButton(f"♻️ Take from Discard ({top_discard_name})", callback_data=f"turn_draw_discard_{player_id}")])

        if game['discard_pile'] and game['discard_pile'][-1].get('type') == 'bottle':
            discarded_bottle_name = game['discard_pile'][-1].get('name', 'Bottle')
            buttons.append([InlineKeyboardButton(f"🍾 Match {discarded_bottle_name}?", callback_data=f"turn_match_discarded_bottle_{player_id}")])
    
    return InlineKeyboardMarkup(buttons)

def get_card_selection_keyboard(
    purpose_prefix: str,
    player_hand: List[Dict],
    player_id_context: Union[int,str],
    facedown: bool = True,
    num_to_select: int = 1,
    min_to_select: int = 1, 
    currently_selected_indices: Optional[List[int]] = None,
    allow_cancel: bool = True,
    cancel_callback_data: Optional[str] = None, 
    blocked_card_indices: Optional[Set[int]] = None 
    ) -> InlineKeyboardMarkup:
    
    buttons_grid: List[List[InlineKeyboardButton]] = []
    flat_buttons: List[InlineKeyboardButton] = []
    if currently_selected_indices is None: currently_selected_indices = []
    if blocked_card_indices is None: blocked_card_indices = set()

    for i in range(len(player_hand)): 
        card_obj = player_hand[i]
        card_name_display = f"Card #{i+1}"
        is_blocked = i in blocked_card_indices
        is_selected = i in currently_selected_indices

        if not facedown: 
            card_name_display = card_obj.get('name', f"Card #{i+1}")
        
        if is_blocked:
            card_name_display += " 🚫(Blocked)"
            callback_data = f"no_action_card_blocked_{i}_{player_id_context}"
        elif is_selected:
            card_name_display += " ✅"
            callback_data = f"{purpose_prefix}_deselect_{i}_{player_id_context}"
        elif len(currently_selected_indices) >= num_to_select and num_to_select > 0: 
            callback_data = f"no_action_max_selected_{i}_{player_id_context}"
        else:
            callback_data = f"{purpose_prefix}_select_{i}_{player_id_context}"
        
        flat_buttons.append(InlineKeyboardButton(card_name_display, callback_data=callback_data))

    idx = 0
    while idx < len(flat_buttons):
        row = flat_buttons[idx : min(idx + 2, len(flat_buttons))]
        buttons_grid.append(row)
        idx += 2
            
    action_row = []
   
    if num_to_select > 0 and min_to_select <= len(currently_selected_indices) <= num_to_select :
        action_row.append(InlineKeyboardButton("✔️ Confirm Selection", callback_data=f"{purpose_prefix}_confirm_{player_id_context}"))
    
    if allow_cancel:
        cancel_cb = cancel_callback_data if cancel_callback_data else f"{purpose_prefix}_cancel_overall_{player_id_context}"
        action_row.append(InlineKeyboardButton("❌ Cancel", callback_data=cancel_cb))
    
    if action_row: buttons_grid.append(action_row)
        
    return InlineKeyboardMarkup(buttons_grid)

def get_target_player_keyboard(
    game_data: Dict,
    ability_user_id: Union[int, str],
    ability_context_key_prefix: str, 
    max_targets: int = 1,
    min_targets: int = 1,
    already_selected_targets: Optional[List[Union[int,str]]] = None,
    exclude_ids: Optional[List[Union[int,str]]] = None,
    cancel_callback_data: Optional[str] = None
    ) -> InlineKeyboardMarkup:

    buttons: List[List[InlineKeyboardButton]] = []
    if already_selected_targets is None: already_selected_targets = []
    current_exclude_ids: List[Union[int,str]] = list(exclude_ids) if exclude_ids is not None else []
    
   
    if ability_user_id not in current_exclude_ids:
        current_exclude_ids.append(ability_user_id) 

    potential_targets = [
        p for p in game_data.get('players', []) + game_data.get('ai_players', [])
        if p['id'] not in current_exclude_ids and p.get('status') == "active"
    ]
    
    temp_row = []
    for target_player in potential_targets:
        display_name = target_player.get('first_name', 'Unknown')
        if target_player.get('is_ai'): display_name = f"🤖 {display_name}" 
        
        callback_data_select = f"{ability_context_key_prefix}_select_target_{target_player['id']}"
        callback_data_deselect = f"{ability_context_key_prefix}_deselect_target_{target_player['id']}"

        button_text = display_name
        button_cb = callback_data_select

        if target_player['id'] in already_selected_targets:
            button_text += " ✅"
            button_cb = callback_data_deselect
        elif len(already_selected_targets) >= max_targets :
             button_cb = f"no_action_max_target_selected_{target_player['id']}"


        temp_row.append(InlineKeyboardButton(button_text, callback_data=button_cb))
        if len(temp_row) == 2: 
            buttons.append(temp_row)
            temp_row = []
    if temp_row: 
        buttons.append(temp_row)
    
    action_row = []
    if min_targets <= len(already_selected_targets) <= max_targets and len(already_selected_targets) > 0 :
        action_row.append(InlineKeyboardButton("✔️ Confirm Target(s)", callback_data=f"{ability_context_key_prefix}_confirm_targets")) 
    
    final_cancel_cb = cancel_callback_data if cancel_callback_data else f"{ability_context_key_prefix}_overall_cancel_{ability_user_id}"

    action_row.append(InlineKeyboardButton("❌ Cancel Ability", callback_data=final_cancel_cb))
    
    if not potential_targets and not any(btn.text.startswith("✔️") for btn_row in buttons for btn in btn_row): 
        if not any(btn.text == "No valid targets available" for btn_row in buttons for btn in btn_row):
             buttons.append([InlineKeyboardButton("No valid targets available", callback_data="no_action_dummy_no_targets")])
    
    if action_row: 
        buttons.append(action_row)
        
    return InlineKeyboardMarkup(buttons)

def get_killer_prompt_keyboard(target_player_id: Union[int, str], killer_context_signature: int) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("🛡️ Use 'The Killer'?", callback_data=f"killer_activate_{target_player_id}_{killer_context_signature}")],
        [InlineKeyboardButton("🚫 Decline", callback_data=f"killer_decline_{target_player_id}_{killer_context_signature}")],
    ]
    return InlineKeyboardMarkup(keyboard)

def get_safe_interaction_keyboard(player_id: Union[int, str]) -> InlineKeyboardMarkup: 
    keyboard = [
        [InlineKeyboardButton("👀 View Safe & Choose Card to Take", callback_data=f"ability_safecracker_view_safe_{player_id}")],
        [InlineKeyboardButton("❌ Cancel Safecracker", callback_data=f"ability_safecracker_overall_cancel_{player_id}")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_safe_card_selection_for_exchange_keyboard(
    safe_cards_count: int, 
    purpose_prefix: str,
    player_id_context: Union[int,str]
    ) -> InlineKeyboardMarkup:
    buttons = []
    for i in range(safe_cards_count):
        buttons.append(InlineKeyboardButton(f"Safe Position #{i+1}", callback_data=f"{purpose_prefix}_select_{i}_{player_id_context}"))
    
    keyboard_layout = []
    for i in range(0, len(buttons), 2): keyboard_layout.append(buttons[i:i+2])
    
    keyboard_layout.append([InlineKeyboardButton("❌ Cancel Safecracker", callback_data=f"ability_safecracker_overall_cancel_{player_id_context}")]) 
    return InlineKeyboardMarkup(keyboard_layout)

def get_bottle_match_prompt_keyboard(
    player_id: Union[int, str],
    hand: List[Dict],
    blocked_card_indices: Optional[Set[int]] = None
    ) -> InlineKeyboardMarkup:
    buttons_grid: List[List[InlineKeyboardButton]] = []
    temp_row: List[InlineKeyboardButton] = []
    if blocked_card_indices is None: blocked_card_indices = set()

    for i, _ in enumerate(hand):
        button_text = f"Card #{i+1}"
        callback_data = f"bottle_match_do_discard_{i}_{player_id}" 

        if i in blocked_card_indices:
            button_text += " 🚫(Blocked)"
            callback_data = f"no_action_card_blocked_{i}_{player_id}" 

        temp_row.append(InlineKeyboardButton(button_text, callback_data=callback_data))
        if len(temp_row) == 2: 
            buttons_grid.append(temp_row)
            temp_row = []
    if temp_row: 
        buttons_grid.append(temp_row)

    buttons_grid.append([InlineKeyboardButton("Skip Matching This Time", callback_data=f"bottle_match_skip_own_{player_id}")])
    return InlineKeyboardMarkup(buttons_grid)

def get_play_again_keyboard() -> InlineKeyboardMarkup:
    keyboard = [[InlineKeyboardButton("🔄 Play Again?", callback_data="main_new_game")]]
    return InlineKeyboardMarkup(keyboard)

def get_confirmation_keyboard(confirm_callback_data: str, cancel_callback_data: str, confirm_text: str = "✅ Yes", cancel_text: str = "❌ No") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(confirm_text, callback_data=confirm_callback_data),
        InlineKeyboardButton(cancel_text, callback_data=cancel_callback_data)
    ]])

def get_gangster_action_type_keyboard(player_id: Union[int, str]) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("MY card vs OPPONENT'S", callback_data=f"ability_gangster_type_own_{player_id}")], 
        [InlineKeyboardButton("OPPONENT vs OPPONENT", callback_data=f"ability_gangster_type_other_{player_id}")],
        [InlineKeyboardButton("❌ Cancel Gangster Operation", callback_data=f"ability_gangster_overall_cancel_{player_id}")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_police_patrol_target_card_keyboard(target_player_hand_count: int, police_player_id: Union[int,str]) -> InlineKeyboardMarkup:
    buttons = []
    temp_row = []
    for i in range(target_player_hand_count):
        callback_data = f"ability_police_chose_card_{i}_{police_player_id}"
        temp_row.append(InlineKeyboardButton(f"Block Target's Card #{i+1}", callback_data=callback_data))
        if len(temp_row) == 2:
            buttons.append(temp_row)
            temp_row = []
    if temp_row:
        buttons.append(temp_row)
    
    action_row = [InlineKeyboardButton("❌ Cancel Police Action", callback_data=f"ability_police_overall_cancel_{police_player_id}")]
    buttons.append(action_row)
    return InlineKeyboardMarkup(buttons)

def get_mole_card_selection_keyboard(
    mole_player_hand: List[Dict], 
    mole_player_id: Union[int, str],
    blocked_card_indices: Optional[Set[int]] = None
    ) -> InlineKeyboardMarkup:
    buttons_grid: List[List[InlineKeyboardButton]] = []
    temp_row: List[InlineKeyboardButton] = []
    if blocked_card_indices is None: blocked_card_indices = set()

    for i, card_obj in enumerate(mole_player_hand):
        button_text = f"Peek at Card #{i+1}"
        callback_action = f"ability_mole_do_peek_{i}_{mole_player_id}" 

        if i in blocked_card_indices:
            pass 

        temp_row.append(InlineKeyboardButton(button_text, callback_data=callback_action))
        if len(temp_row) == 2: 
            buttons_grid.append(temp_row)
            temp_row = []
    if temp_row:
        buttons_grid.append(temp_row)
    
    buttons_grid.append([InlineKeyboardButton("❌ Cancel Mole Action", callback_data=f"ability_mole_overall_cancel_{mole_player_id}")])
    
    return InlineKeyboardMarkup(buttons_grid)

