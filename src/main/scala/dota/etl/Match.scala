package dota.etl

case class Match(
                  Id: Long,
                  PlayerSlot: Short,
                  RadiantWin: Boolean,
                  //                  Duration: Short,
                  //                  GameMode: Short,
                  //                  LobbyType: Short,
                  //                  HeroId: Short,
                  //                  StartTime: Long,
                  //                  Version: Short,
                  Kills: Short,
                  Deaths: Short,
                  Assists: Short
                  //                  Skill: Short,
                  //                  XpPerMin: Short,
                  //                  GoldPerMin: Short,
                  //                  HeroDamage: Int,
                  //                  TowerDamage: Int,
                  //                  HeroHealing: Int,
                  //                  LastHits: Short,
                  //                  Lane: Short,
                  //                  LaneRole: Short,
                  //                  IsRoaming: Boolean,
                  //                  Cluster: Short,
                  //                  LeaverStatus: Short,
                  //                  PartySize: Short
                )
