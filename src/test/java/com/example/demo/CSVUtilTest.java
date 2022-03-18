package com.example.demo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVUtilTest {

    private static final Logger log= LoggerFactory.getLogger(Player.class);

    @Test
    void converterData(){
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }

    @Test
    void stream_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .flatMap(playerA ->list.parallelStream()
                        .filter(playerB -> playerA.club.equals(playerB.club))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getClub));
        listFilter.values().stream().forEach((players)->
                players.stream().forEach(player -> {
                    System.out.println("nombre: "+player.getName() + " club: " + player.getClub() + " age: "+player.getAge());
                })
                );
        assert listFilter.size() == 322;

    }


    @Test
    void reactive_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                         .filter(playerB -> playerA.stream()
                                 .anyMatch(a ->  a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);
        listFilter.subscribe();
        assert listFilter.block().size() == 322;

    }

    @Test
    void reactive_filtrarJugadoresMayoresA34() {
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        //Publicador
        Mono<Map<String, Collection<Player>>> MayoresA34 = listFlux
                .flatMap(playerA -> listFlux
                        .filter(player -> player.age > 34))
                .distinct()
                .collectMultimap(Player::getName);

        //subcripcion a observable (observador)
        MayoresA34.subscribe(p -> System.out.println("Numero de jugadores Mayores a 34: " + p.size()));
        //impresion del nombre y edad
        MayoresA34.block().forEach((nombre, player) -> {
            player.forEach(p -> {
                System.out.println(
                        "[Nombre: " + p.getName() + " ]" + " [ Edad: " + p.getAge() + "]"
                );
            });
        });
        assert MayoresA34.block().size() == 488;
    }
        @Test
        void reactive_FiltrarPorClub(){
            List<Player> list = CsvUtilFile.getPlayers();
            Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
            //Publicador
            Mono<Map<String, Collection<Player>>> filtroPorClub = listFlux
                    .flatMap(playerA -> listFlux
                            .filter(player -> player.club.equals("FC Barcelona")))
                    .distinct()
                    .collectMultimap(Player::getName);

            //subcripcion a observable (observador)
            filtroPorClub.subscribe(p -> System.out.println("Numero de jugadores del FC Barcelona: " + p.size()));
            //impresion del nombre y edad
            filtroPorClub.block().forEach((nombre, player) -> {
                player.forEach(p -> {
                    System.out.println(
                            "[Nombre: " + p.getName() + " ]" + " [ club: " + p.getClub() + "]"
                    );
                });
            });
            assert filtroPorClub.block().size() == 33;
    }

    @Test
    void ranckingPorPais(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        var ranckingPorPais= listFlux
                .groupBy(player -> player.getNational())
                .flatMap(player->player.buffer());

        Mono<Map<String, Collection<Player>>> filtroPorJugador = listFlux
                .flatMap(playerA -> listFlux
                        .filter(player -> player.name.equals("Sergio Busquets")))
                .distinct()
                .collectMultimap(Player::getName);

        //Subscriptor muestra de nacionalidad de jugador
        filtroPorJugador.subscribe(p->p.forEach((nombre,players)->{
            players.forEach(player -> System.out.println("[ Nombre Jugador: " + player.getName() + " ]"
                + " [ Nacionalidad: " + player.getNational() + " ]"
            ));
        }));
        //Subscriptor Muestra Rancking de jugadores por pais
        ranckingPorPais.subscribe(p->p.forEach(r->
                System.out.println("[Nombre: " + r.getName() +" ]" + " [Pais: " + r.getNational() + " ]" )
                ));

    }

}
