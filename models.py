from pydantic import BaseModel
from typing import List
from typing import Optional

class Player(BaseModel):
  id: str
  address: str

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
  
class Game(BaseModel):
  id: str
  week_id: str
  contract_address: str
  name: str
  scheduled: str
  status: str
  players: List[Player] = []
  claimed_squares: Optional[List[Square]] = []
  payouts: Optional[List[Square]] = []
  scoring_plays: Optional[List[ScoringPlay]] = [],
  buyin: Optional[float] = 4.00