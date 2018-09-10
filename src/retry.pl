% execute with SWI-Prolog:
%
% generate the when clauses for matching
% swipl -q -l retry.pl -t to_whens
%
% generate the map for dependency checking
% swipl -q -l retry.pl -t to_map

:- use_module(library(bounds)).

% The complete list as of 04-Sep-2018
all_b2api(b2_authorize_account).
all_b2api(b2_cancel_large_file).
all_b2api(b2_create_bucket).
all_b2api(b2_create_key).
all_b2api(b2_delete_bucket).
all_b2api(b2_delete_file_version).
all_b2api(b2_delete_key).
all_b2api(b2_download_file_by_id).
all_b2api(b2_download_file_by_name).
all_b2api(b2_finish_large_file).
all_b2api(b2_get_download_authorization).
all_b2api(b2_get_file_info).
all_b2api(b2_get_upload_part_url).
all_b2api(b2_get_upload_url).
all_b2api(b2_hide_file).
all_b2api(b2_list_buckets).
all_b2api(b2_list_file_names).
all_b2api(b2_list_file_versions).
all_b2api(b2_list_keys).
all_b2api(b2_list_parts).
all_b2api(b2_list_unfinished_large_files).
all_b2api(b2_start_large_file).
all_b2api(b2_update_bucket).
all_b2api(b2_upload_file).
all_b2api(b2_upload_part ).

% All api calls we're interested in.
% In approximate frequency of usage order.
some_b2api(b2_upload_part).
some_b2api(b2_get_upload_part_url).
some_b2api(b2_get_upload_url).
some_b2api(b2_upload_file).
some_b2api(b2_authorize_account).
some_b2api(b2_list_buckets).
some_b2api(b2_list_file_names).
some_b2api(b2_delete_file_version).
some_b2api(b2_finish_large_file).
some_b2api(b2_start_large_file).

b2api(A) :- some_b2api(A).

% Needs library(bounds). Leaves the bounds as a predicate, which is nicer to
% read, can be output as a ruby range, and still matches correctly.
five_hundreds(V) :- V in 500..599.

% these two have special retries
upload_retry(b2_upload_part, b2_get_upload_part_url).
upload_retry(b2_upload_file, b2_get_upload_url).

% These two codes from 401 are allowed to retry
not_authorised_retryable_code(expired_auth_token).
not_authorised_retryable_code(bad_auth_token).

% Sources:
% https://www.backblaze.com/b2/docs/calling.html
% https://www.backblaze.com/b2/docs/uploading.html
% https://www.backblaze.com/b2/docs/integration_checklist.html

%%%%%%%%%%%%%%
% HttpStatus is one of the Http status codes, as an integer
% Code is a backblaze-specific string, read from the response body
% RetryCalls is the set of calls to use to retry the OriginalCall
% retry(OriginalCall, HttpStatus, Code, RetryCalls)

% No retry, cos it succeeded :-D
retry(_, HttpStatus, _, []) :- HttpStatus in 200..399, false.

% upload-specific 401 :- re-fetch upload url.
%
% Some docs say only for expired_auth_token? Other docs say just retry
% regardless of code.
retry( Call, 401, Code, [Retry,Call] ) :-
  not_authorised_retryable_code(Code),
  upload_retry(Call,Retry).

% upload-specific 408,5xx - re-fetch upload url
retry( Call, HttpStatus, _Code, [Retry,Call] ) :-
  (HttpStatus = 408; five_hundreds(HttpStatus)),
  upload_retry(Call,Retry).

% upload-specific 429 - just retry the call
retry( Call, 429, _Code, [Call] ) :- upload_retry(Call,_).

% non-upload 401 failures - re-auth account
retry( Call, 401, Code, [b2_authorize_account,Call] ) :-
  Call \= b2_authorize_account, % b2_authorize_account can't retry with b2_authorize_account
  not(upload_retry(Call,_)),
  not_authorised_retryable_code(Code),
  not(upload_retry(Call,_)).

% non-upload with 408, 429, and 5xx - retry call (after a backoff)
retry( Call,HttpStatus,_Code,[Call] ) :-
  not(upload_retry(Call,_)),
  (member(HttpStatus, [408,429]); five_hundreds(HttpStatus)),
  not(upload_retry(Call,_)).

%%%%%%%%%%%%%%%%%%%
% generate all posibilities
allballs( Call,HttpStatus,Code,Retry ) :-
  b2api(Call),
  retry(Call,HttpStatus,Code,Retry).

%%%%%%%%%%%%%%%%%%%
% Various conversions to ruby

% use with with_output_to, can't figure out another way :-\
to_ruby_commas([]).
to_ruby_commas([Last]) :- !, to_ruby(Last).
to_ruby_commas([Fst|Rst]) :- to_ruby(Fst), format(','), to_ruby_commas(Rst).

% This has to be before ruby_lit(A). Dunno why.
to_ruby(Range) :-       attvar(Range), get_attr(Range, bounds, bounds(N,X,_)), format('~d..~d', [N,X]).
to_ruby(A) :-           atom(A),       format(':~a', A).
to_ruby(S) :-           string(S),     format('"~s"', S).
to_ruby(N) :-           integer(N),    format('~d', N).
to_ruby(F) :-           float(F),      format( '~f', F).
to_ruby(L) :-           is_list(L),    format('[~@]', to_ruby_commas(L)).
to_ruby(ruby_lit(A)) :- format('~w', A).

default_to_any(Code,DefCode) :- ground(Code) -> format(atom(DefCode), ':~a', Code); DefCode = 'Any'.

all_whens :-
  allballs( Call, HttpStatus, Code, Retries ),
  to_when( Call, HttpStatus, Code, Retries ).

to_when( Call, HttpStatus, Code, Retries ) :-
  default_to_any(Code,DefaultedAnyCode),
  format('when [~@, ~@, ~a] then ~@~n', [to_ruby(Call),to_ruby(HttpStatus),DefaultedAnyCode,to_ruby(Retries)] ).

to_whens :- findall(_,all_whens,_).
to_whens(Term) :- findall(_,(apply(Term,C,Hs,Co,Re),retry(C,Hs,Co,Re)),_).

some_whens :-
  findall(
    _,
    (member(C,[b2_upload_part,b2_upload_file,b2_authorize_account,any_call]),retry(C,Hs,Co,Re),to_when(C,Hs,Co,Re))
    ,_).

map_all :-
  format('retries = Hash.new{|h,k| h[k] = Set.new}~n'),
  allballs( Call, _HttpStatus, _Code, Retries ),
  selectchk(Call,Retries,Uniqs),
  Uniqs \= [],
  format('retries[~@].merge(~@)~n', [to_ruby(Call),to_ruby(Uniqs)] ).

to_map :- findall(_,map_all,_), format('retries~n').
