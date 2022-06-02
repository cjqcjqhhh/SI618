- The “movie_genre” table, which has two columns: `imdb_id` and `genre`. A movie typically has multiple genres, and in this case, there should be one row for each genre. If some movie does not have any genre, ignore that movie.  

  `movie_genre`:

  |      |   imdb_id |    genre |
  | :--- | --------: | -------: |
  | 0    | tt0111161 |    Crime |
  | 0    | tt0111161 |    Drama |
  | 1    | tt0068646 |    Crime |
  | 1    | tt0068646 |    Drama |
  | 2    | tt0071562 |    Crime |
  | ...  |       ... |      ... |
  | 246  | tt0036613 |   Comedy |
  | 246  | tt0036613 |    Crime |
  | 246  | tt0036613 |  Romance |
  | 246  | tt0036613 | Thriller |
  | 247  | tt1454029 |    Drama |

- The “movies” table, which has four columns: `imdb_id`, `title`, `year`, `rating`, `country` 

  `movies`:

  |      |   imdb_id |                            title | year | rating |                           country |
  | :--- | --------: | -------------------------------: | ---: | -----: | --------------------------------: |
  | 0    | tt0111161 |         The Shawshank Redemption | 1994 |    9.3 |                             [USA] |
  | 1    | tt0068646 |                    The Godfather | 1972 |    9.2 |                             [USA] |
  | 2    | tt0071562 |           The Godfather: Part II | 1974 |    9.1 |                             [USA] |
  | 3    | tt0110912 |                     Pulp Fiction | 1994 |    9.0 |                             [USA] |
  | 4    | tt0060196 | Il buono, il brutto, il cattivo. | 1966 |    9.0 |      [Italy, Spain, West Germany] |
  | ...  |       ... |                              ... |  ... |    ... |                               ... |
  | 243  | tt0092067 |           Tenkû no shiro Rapyuta | 1986 |    8.1 |                           [Japan] |
  | 244  | tt0094226 |                 The Untouchables | 1987 |    8.0 |                             [USA] |
  | 245  | tt0036342 |                Shadow of a Doubt | 1943 |    8.0 |                             [USA] |
  | 246  | tt0036613 |             Arsenic and Old Lace | 1944 |    8.0 |                             [USA] |
  | 247  | tt1454029 |                         The Help | 2011 |    8.0 | [USA, India, United Arab Emirates |

- The “movie_actor” table, which has two columns `imdb_id` and `actor`. A movie typically has   multiple actors, and in this case, there should be one row for each actor.

  `movie_actor`:

  |      |   imdb_id |          actor |
  | :--- | --------: | -------------: |
  | 0    | tt0111161 |    Tim Robbins |
  | 0    | tt0111161 | Morgan Freeman |
  | 0    | tt0111161 |     Bob Gunton |
  | 0    | tt0111161 | William Sadler |
  | 0    | tt0111161 |   Clancy Brown |
  | ...  |       ... |            ... |
  | 247  | tt1454029 |   Chris Lowell |
  | 247  | tt1454029 |   Cicely Tyson |
  | 247  | tt1454029 |     Mike Vogel |
  | 247  | tt1454029 |   Sissy Spacek |
  | 247  | tt1454029 |   Brian Kerwin |













| rating | genres |          rated | filming_locations |                                          language |                              title |                  runtime |    poster |                                 imdb_url |                              writers |                                           imdb_id | directors |           rating_count | actors |                                       plot_simple |                                              year | country |  type | release_date | also_known_as |                                       |
| :----- | -----: | -------------: | ----------------: | ------------------------------------------------: | ---------------------------------: | -----------------------: | --------: | ---------------------------------------: | -----------------------------------: | ------------------------------------------------: | --------: | ---------------------: | -----: | ------------------------------------------------: | ------------------------------------------------: | ------: | ----: | -----------: | ------------: | ------------------------------------- |
| 0      |    9.3 | [Crime, Drama] |                 R |                                Ashland, Ohio, USA |                          [English] | The Shawshank Redemption | [142 min] | http://img3.douban.com/lpic/s1311361.jpg | http://www.imdb.com/title/tt0111161/ |                    [Stephen King, Frank Darabont] | tt0111161 |       [Frank Darabont] | 894012 | [Tim Robbins, Morgan Freeman, Bob Gunton, Will... | Two imprisoned men bond over a number of years... |    1994 | [USA] |            M |      19941014 | [Die Verurteilten]                    |
| 1      |    9.2 | [Crime, Drama] |                 R | 110 Longfellow Road, Staten Island, New York C... |          [English, Italian, Latin] |            The Godfather | [175 min] | http://img3.douban.com/lpic/s4038344.jpg | http://www.imdb.com/title/tt0068646/ | [Mario Puzo, Francis Ford Coppola, and 1 more ... | tt0068646 | [Francis Ford Coppola] | 646348 | [Marlon Brando, Al Pacino, James Caan, Richard... | The aging patriarch of an organized crime dyna... |    1972 | [USA] |            M |      19720324 | [Mario Puzo's The Godfather]          |
| 2      |    9.1 | [Crime, Drama] |                 R | 6th Street, Manhattan, New York City, New York... | [English, Italian, Spanish, Latin] |   The Godfather: Part II | [200 min] | http://img3.douban.com/lpic/s3314652.jpg | http://www.imdb.com/title/tt0071562/ | [Francis Ford Coppola, Mario Puzo, and 1 more ... | tt0071562 | [Francis Ford Coppola] | 415305 | [Al Pacino, Robert Duvall, Diane Keaton, Rober... | The early life and career of Vito Corleone in ... |    1974 | [USA] |            M |      19741220 | [Mario Puzo's The Godfather: Part II] |