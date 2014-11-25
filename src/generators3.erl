-module(generators3).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([from_fun/2, seq/2, from_list/1, chain/1, from_ets/1, from_ets/3]).
-export([map/2, pmap/2, pmap/3, filter/2]).
-export([next/1, take/2, materialize/1]).


%%
%% GENERATORS
%%

from_fun(F, Acc0) ->
    {Acc0, fun (Acc) -> F(Acc) end}.

seq(Start, End) when Start =< End ->
    Seq = fun (empty)  -> empty;
              ({E, E}) -> {E, empty};
              ({S, E}) -> {S, {S+1, E}}
          end,
    {{Start, End}, Seq}.


from_list(L) ->
    {L, fun ([]) -> empty;
            ([H | T]) -> {H, T}
        end}.

chain(Gens) ->
    F = fun F([G]) ->
                case next(G) of
                    {V, NewG} ->
                        {V, [NewG]};
                    empty ->
                        empty
                end;
            F([G | Rest]) ->
                case next(G) of
                    {V, NewG} ->
                        {V, [NewG | Rest]};
                    empty ->
                        F(Rest)
                end
        end,
    {Gens, F}.


from_ets(Table) ->
    from_ets(Table, [{'$1', [], ['$1']}], 10).

from_ets(Table, MatchSpec, Limit) ->
    F = fun (undefined) ->
                case ets:select(Table, MatchSpec, Limit) of
                    {[], _} ->
                        empty;
                    {Matches, Cont} ->
                        {V, MatchGen} = next(from_list(Matches)),
                        {V, {MatchGen, Cont}};
                    '$end_of_table' ->
                        empty
                end;
            ({MatchGen, Cont}) ->
                case next(MatchGen) of
                    {V, NewMatchGen} ->
                        {V, {NewMatchGen, Cont}};
                    empty ->
                        case ets:select(Cont) of
                            {Matches, NewCont} ->
                                case next(from_list(Matches)) of
                                    {V, NewMatchGen} ->
                                        {V, {NewMatchGen, NewCont}};
                                    empty ->
                                        throw(unexpected_empty)
                                end;
                            '$end_of_table' ->
                                empty
                        end
                end
        end,

    {undefined, F}.


%%
%% TRANSFORMERS
%%

map(F, Gen) ->
    {Gen, fun (G) ->
                  case next(G) of
                      {V, NewG} -> {F(V), NewG};
                      empty     -> empty
                  end
          end}.

pmap(F, Gen) ->
    pmap(F, Gen, []).

%% For each element in the given generator, a process is spawned to
%% evaluate F(E). Concurrency is controlled by the 'workers'
%% option. Calling next/1 on this generator blocks until a worker
%% finishes processing (or times out) and a result is available. Order
%% is not preserved. This is just a simple hack, it needs to be
%% extended with a middleman process and chunking, as in stdlib2
%% s2_par.
pmap(F, Gen, Opts) ->
    Workers = proplists:get_value(workers, Opts, 1),
    Timeout = proplists:get_value(timeout, Opts, infinity),
    Parent = self(),

    SpawnWorker = fun (V) ->
                          proc_lib:spawn_link(fun () ->
                                                      Parent ! {self(), F(V)}
                                              end)
                  end,

    CollectResult = fun ([], 0, empty) ->
                            empty;
                        (Pids, NumWorkers, G) ->
                            receive
                                {Pid, Result} ->
                                    true = lists:member(Pid, Pids),
                                    NewPids = lists:delete(Pid, Pids),
                                    {Result, {NewPids, NumWorkers-1, G}}
                            after Timeout ->
                                    {error, timeout}
                            end
                    end,

    Map = fun Map({Pids, NumWorkers, empty}) ->
                  CollectResult(Pids, NumWorkers, empty);
              Map({Pids, NumWorkers, G}) when NumWorkers < Workers ->

                  case next(G) of
                      {V, NewG} ->
                          Pid = SpawnWorker(V),
                          Map({[Pid | Pids], NumWorkers+1, NewG});
                      empty ->
                          CollectResult(Pids, NumWorkers, empty)
                  end;
              Map({Pids, NumWorkers, G}) ->
                  CollectResult(Pids, NumWorkers, G)
          end,

    {{[], 0, Gen}, Map}.


filter(F, Gen) ->
    {Gen, fun Filter(G) ->
                  case next(G) of
                      {V, NewG} ->
                          case F(V) of
                              true  -> {V, NewG};
                              false -> Filter(NewG)
                          end;
                      empty ->
                          empty
                  end
             end}.


%%
%% USER API
%%

next({State, F}) ->
    case F(State) of
        empty         -> empty;
        {V, NewState} -> {V, {NewState, F}}
    end.

take(N, Gen) ->
    take(N, Gen, []).

take(0, Gen, Acc) ->
    {lists:reverse(Acc), Gen};
take(N, Gen, Acc) ->
    case next(Gen) of
        {V, NewGen} ->
            take(N-1, NewGen, [V | Acc]);
        empty ->
            {lists:reverse(Acc), Gen}
    end.

materialize(Gen) ->
    case next(Gen) of
        {V, NewGen} ->
            [V | materialize(NewGen)];
        empty ->
            []
    end.

%%
%% TESTS
%%

-ifdef(TEST).
seq_test() ->
    [1, 2, 3] = materialize(seq(1, 3)).

map_test() ->
    [2, 4, 6] = materialize(map(fun (I) -> I*2 end, seq(1, 3))).

pmap_test() ->
    [2, 4, 6] = materialize(pmap(fun (I) -> I*2 end, seq(1, 3))),

    Seq = lists:seq(1, 10),
    Seq = lists:sort(materialize(pmap(fun (I) -> timer:sleep(10), I end,
                                      seq(1, 10),
                                      [{workers, 10}]))),
    ok.

filter_test() ->
    [2, 4, 6] = materialize(filter(fun (I) -> I rem 2 =:= 0 end, seq(1, 7))).

from_list_test() ->
    [1, 2, 3] = materialize(from_list([1, 2, 3])).

ets_test() ->
    Table = ets:new(foo, [ordered_set]),
    Objects = [{I} || I <- lists:seq(1, 100)],
    ets:insert(Table, Objects),
    Objects = materialize(from_ets(Table)),

    Mult = [I*2 || {I} <- Objects],
    Mult = materialize(map(fun ({I}) -> I*2 end, from_ets(Table))).

from_fun_test() ->
    [0, 1, 2] = materialize(from_fun(fun (Acc) when Acc =:= 3 -> empty;
                                         (Acc) -> {Acc, Acc+1}
                                     end, 0)).

chain_test() ->
    G = chain([from_list([1, 2, 3]), from_list([4, 5, 6])]),
    [1, 2, 3, 4, 5, 6] = materialize(G).

take_test() ->
    {[1, 2, 3], G0} = take(3, seq(1, 6)),
    [4, 5, 6] = materialize(G0),
    {[4, 5, 6], G1} = take(3, G0),
    {[], G1} = take(3, G1),
    empty = next(G1).

-endif.
