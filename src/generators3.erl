-module(generators3).
-include_lib("eunit/include/eunit.hrl").

%%
%% GENERATORS
%%

from_fun(F, Acc0) ->
    {Acc0, fun (Acc) -> F(Acc) end}.

seq(Start, End) when Start =< End ->
    Seq = fun (empty) -> empty;
              ({E, E}) -> {E, empty};
              ({S, E}) -> {S, {S+1, E}}
          end,
    {{Start, End}, Seq}.


from_list(L) ->
    {L, fun ([]) -> empty;
            ([H | T]) -> {H, T}
        end}.

when_empty(Current, Next) ->
    F = fun F({empty, N}) ->
                case next(N) of
                    {V, NewN} ->
                        {V, {empty, NewN}};
                    empty ->
                        empty
                end;

            F({C, N}) ->
                case next(C) of
                    {V, NewC} ->
                        {V, {NewC, N}};
                    empty ->
                        F({empty, N})
                end
        end,
    {{Current, Next}, F}.


from_ets(Table) ->
    from_ets(Table, [{'$1', [], ['$1']}], 10).

from_ets(Table, MatchSpec, Limit) ->
    F = fun F(undefined) ->
                case ets:select(Table, MatchSpec, Limit) of
                    {Matches, Cont} ->

                        case next(from_list(Matches)) of
                            {V, MatchGen} ->
                                {V, {MatchGen, Cont}};
                            empty ->
                                throw(unexpected_empty)
                        end;
                    '$end_of_table' ->
                        empty
                end;
            F({MatchGen, Cont}) ->
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


filter(F, Gen) ->
    {Gen, fun Filter(G) ->
                  case next(G) of
                      {V, NewG} ->
                          case F(V) of
                              true ->
                                  {V, NewG};
                              false ->
                                  Filter(NewG)
                          end;
                      empty ->
                          empty
                  end
             end}.

next({State, F}) ->
    case F(State) of
        empty -> empty;
        next -> next;
        {V, NewState} ->
            {V, {NewState, F}}
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



seq_test() ->
    [1, 2, 3] = materialize(seq(1, 3)).

map_test() ->
    [2, 4, 6] = materialize(map(fun (I) -> I*2 end, seq(1, 3))).

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
