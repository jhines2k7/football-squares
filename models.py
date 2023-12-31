from pydantic import BaseModel
from typing import List
from typing import Optional

class Player(BaseModel):
  id: str
  address: str
  games: List[str]
  week_id: str

class Square(BaseModel):
  id: str
  home_points: int
  away_points: int
  player_id: str
  game_id: str
  week_id: str
  paid: Optional[bool] = False

class ScoringPlay(BaseModel):
  id: str
  type: str
  play_type: str
  home_points: int
  away_points: int
  home_team: str
  away_team: str
  offset: Optional[int] = 0
  week_id: str
  event_num: int
  game_id: str
  
class Game(BaseModel):
  id: str
  week_id: str
  contract_address: str
  name: str
  scheduled: str
  status: str
  players: List[str]
  claimed_squares: Optional[List[Square]] = []
  payouts: Optional[List[Square]] = []
  scoring_plays: Optional[List[ScoringPlay]] = [],
  buyin: Optional[int] = 0

class ScoringPlayDTO(BaseModel):
  event_num: Optional[int] = -1
  week_id: str
  game_id: str
  scoring_play: ScoringPlay