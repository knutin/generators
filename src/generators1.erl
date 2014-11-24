-module(generators1).
-include_lib("eunit/include/eunit.hrl").


seq(End, End) ->
    fun () -> {End, empty()} end;
seq(Start, End) when Start =< End ->
    fun () -> {Start, seq(Start+1, End)} end.


map(F, Gen) ->
    fun () ->
            case next(Gen) of
                empty       -> empty;
                {V, NewGen} -> {F(V), map(F, NewGen)}
            end
    end.

filter(Pred, Gen) ->
    %% Consume until predicate returns true, might consume entire
    %% generator
    NextTrue = fun F(P, G) ->
                          case next(G) of
                              empty -> empty;
                              {V, NewG} ->
                                  case P(V) of
                                      true  -> {V, NewG};
                                      false -> F(P, NewG)
                                  end
                          end
                  end,

    fun () ->
            case NextTrue(Pred, Gen) of
                empty -> empty;
                {V, NewGen} ->
                    {V, filter(Pred, NewGen)}
            end
    end.


next(Gen) ->
    case Gen() of
        empty -> empty;
        {V, NewGen} -> {V, NewGen}
    end.

from_list([])      -> empty();
from_list([H | T]) -> fun () -> {H, from_list(T)} end.


from_ets(Table) ->
    from_ets(Table, [{'$1', [], ['$1']}], 10).

from_ets(Table, MatchSpec, Limit) ->
    %% Does this consume the entire table immediately?
    SelectGen = fun F(undefined) ->
                        case ets:select(Table, MatchSpec, Limit) of
                            {Matches, Cont} ->
                                next(when_empty(from_list(Matches), F(Cont)));
                            '$end_of_table' ->
                                empty
                        end;
                    F(Cont) ->
                        case ets:select(Cont) of
                            {Matches, NewCont} ->
                                when_empty(from_list(Matches), F(NewCont));
                            '$end_of_table' ->
                                empty()
                        end
                end,
    fun () -> SelectGen(undefined) end.

from_fun(F, Acc) ->
    fun () ->
            case F(Acc) of
                empty       -> empty;
                {V, NewAcc} -> {V, from_fun(F, NewAcc)}
            end
    end.




%%
%% INTERNAL HELPERS
%%

empty() -> fun () -> empty end.


materialize(Gen) ->
    case next(Gen) of
        empty       -> [];
        {V, NewGen} -> [V | materialize(NewGen)]
    end.

when_empty(Gen, NextGen) ->
    fun () ->
            case next(Gen) of
                empty ->
                    next(NextGen);
                {V, NewGen} ->
                    {V, when_empty(NewGen, NextGen)}
            end
    end.


%%
%% TESTS
%%

seq_test() ->
    [1, 2, 3] = materialize(seq(1, 3)).

map_test() ->
    [2, 4, 6] = materialize(map(fun (V) -> V*2 end,
                                seq(1, 3))).

next_test() ->
    G0 = map(fun (V) -> V*2 end, seq(1, 3)),
    {2, G1} = next(G0),
    {4, G2} = next(G1),
    {6, G3} = next(G2),
    empty = next(G3).

from_list_test() ->
    [1, 2, 3] = materialize(from_list([1, 2, 3])).


filter_test() ->
    Pred = fun (V) -> V rem 2 =:= 0 end,
    [2, 4] = materialize(filter(Pred, from_list([2, 3, 4]))),
    [2, 4, 6, 8, 10] = materialize(filter(Pred, seq(1, 10))).

when_empty_test() ->
    [1, 2, 3, 4] = materialize(when_empty(from_list([1, 2]), from_list([3, 4]))).


ets_test() ->
    Table = ets:new(foo, [ordered_set]),
    Objects = [{I, foo} || I <- lists:seq(1, 100)],
    ets:insert(Table, Objects),
    Objects = materialize(from_ets(Table)),

    Mult = [I*2 || {I, foo} <- Objects],
    Mult = materialize(map(fun ({I, foo}) -> I*2 end, from_ets(Table))).

from_fun_test() ->
    AccF = fun (Acc) when Acc =:= 3 -> empty;
               (Acc) -> {Acc, Acc+1}
           end,
    [0, 1, 2] = materialize(from_fun(AccF, 0)).
