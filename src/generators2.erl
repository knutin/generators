-module(generators2).
-include_lib("eunit/include/eunit.hrl").

%%
%% GENERATORS
%%

seq(Start, End) when Start =< End ->
    F = fun (empty) -> throw(empty);
            ({E, E}) -> {E, empty};
            ({S, E}) -> {S, {S+1, E}}
        end,
    {[{Start, End}], [F]}.

from_list(L) ->
    {[L], [fun ([]) -> throw(empty);
               ([H | T]) -> {H, T} end]}.


%% when_empty(Gen, NextGen) ->
%%     {[{Gen, NextGen}], [fun ({G, N}) ->
%%                                 case next(G) of
%%                                     empty ->
%%                                         case next(N) of
%%                                             empty ->
%%                                                 throw(empty);
%%                                             {V, NewN} ->
%%                                                 {V, {[N]


%%     {[F | ArgStack], [fun (E, ArgF) -> {ArgF(E), ArgF} end | GenStack]}.

%%     fun () ->
%%             case next(Gen) of
%%                 empty ->
%%                     next(NextGen);
%%                 {V, NewGen} ->
%%                     {V, when_empty(NewGen, NextGen)}
%%             end
%%     end.




next({ArgStack, GenStack}) ->
    try
        do_next(ArgStack, GenStack)
    catch
        throw:empty ->
            empty
    end.

do_next([Arg], [Gen]) ->
    {Value, NewArg} = Gen(Arg),
    {Value, {[NewArg], [Gen]}};

do_next([Arg | ArgStack], [Gen | GenStack]) ->
    {Value, {NewArgStack, NewGenStack}} = do_next(ArgStack, GenStack),
    try
        {NewValue, NewArg} = Gen(Value, Arg),
        {NewValue, {[NewArg | NewArgStack], [Gen | NewGenStack]}}
    catch
        throw:pass ->
            do_next([Arg | NewArgStack], [Gen | NewGenStack])
    end.



%%
%% TRANSFORMERS
%%

map(F, {ArgStack, GenStack}) ->
    {[F | ArgStack], [fun (E, ArgF) -> {ArgF(E), ArgF} end | GenStack]}.



filter(Pred, {ArgStack, GenStack}) ->
    {[Pred | ArgStack], [fun (E, ArgPred) -> case ArgPred(E) of
                                                 true ->
                                                     {E, ArgPred};
                                                 false ->
                                                     throw(pass)
                                             end
                         end | GenStack]}.


%% filter(Pred, {ArgStack, GenStack}) ->
%%     %% Consume until predicate returns true, might consume entire
%%     %% generator
%%     NextTrue = fun F(P, G) ->
%%                           case next(G) of
%%                               empty -> empty;
%%                               {V, NewG} ->
%%                                   case P(V) of
%%                                       true  -> {V, NewG};
%%                                       false -> F(P, NewG)
%%                                   end
%%                           end
%%                   end,

%%     fun () ->
%%             case NextTrue(Pred, Gen) of
%%                 empty -> empty;
%%                 {V, NewGen} ->
%%                     {V, filter(Pred, NewGen)}
%%             end
%%     end.





%% from_list([])      -> empty();
%% from_list([H | T]) -> fun () -> {H, from_list(T)} end.




%% from_ets(Table) ->
%%     from_ets(Table, [{'$1', [], ['$1']}], 10).

%% from_ets(Table, MatchSpec, Limit) ->
%%     SelectGen = fun F(undefined) ->
%%                         case ets:select(Table, MatchSpec, Limit) of
%%                             {Matches, Cont} ->
%%                                 next(when_empty(from_list(Matches), F(Cont)));
%%                             '$end_of_table' ->
%%                                 empty
%%                         end;
%%                     F(Cont) ->
%%                         case ets:select(Cont) of
%%                             {Matches, NewCont} ->
%%                                 when_empty(from_list(Matches), F(NewCont));
%%                             '$end_of_table' ->
%%                                 empty()
%%                         end
%%                 end,
%%     fun () -> SelectGen(undefined) end.

%% from_fun(F, Acc) ->
%%     fun () ->
%%             case F(Acc) of
%%                 empty       -> empty;
%%                 {V, NewAcc} -> {V, from_fun(F, NewAcc)}
%%             end
%%     end.




%%
%% INTERNAL HELPERS
%%

empty() -> fun () -> empty end.


materialize(Gen) ->
    case next(Gen) of
        empty       -> [];
        {V, NewGen} -> [V | materialize(NewGen)]
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

%% when_empty_test() ->
%%     [1, 2, 3, 4] = materialize(when_empty(from_list([1, 2]), from_list([3, 4]))).


%% ets_test() ->
%%     Table = ets:new(foo, [ordered_set]),
%%     Objects = [{I, foo} || I <- lists:seq(1, 100)],
%%     ets:insert(Table, Objects),
%%     Objects = materialize(from_ets(Table)),

%%     Mult = [I*2 || {I, foo} <- Objects],
%%     Mult = materialize(map(fun ({I, foo}) -> I*2 end, from_ets(Table))).

%% from_fun_test() ->
%%     AccF = fun (Acc) when Acc =:= 3 -> empty;
%%                (Acc) -> {Acc, Acc+1}
%%            end,
%%     [0, 1, 2] = materialize(from_fun(AccF, 0)).
                                             
