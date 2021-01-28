package com.bolsadeideas.springboot.reactor.app;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import javax.management.RuntimeErrorException;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.bolsadeideas.springboot.reactor.app.models.Comentarios;
import com.bolsadeideas.springboot.reactor.app.models.Usuario;
import com.bolsadeideas.springboot.reactor.app.models.UsuarioComentarios;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
//Implementamos la interfaceCommandLineRunner para que la aplicación sea de consola o de linea de comando
@SpringBootApplication
public class SpringBootReactorApplication implements CommandLineRunner {

	private static final Logger log	= LoggerFactory.getLogger(SpringBootReactorApplication.class);
	
	public static void main(String[] args) {
		SpringApplication.run(SpringBootReactorApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
//		ejemplosVarios();
//		ejemploFlatMap();
//		ejemploToString();
//		ejemploCollectList2();
//		ejemploUsuarioComentariosFlatMap();
//		ejemploUsuarioComentariosZipWith2();
//		ejemploUsuarioComentariosZipWithForma2();
//		ejemploZipWithRangos();
//		ejemploZipWithRangos2();
//		ejemploInterval();
//		ejemploIntervalConBloqueo();
//		ejemploDelayElements();
//		ejemploDelayElements2();
//		ejemploIntervalInfinito();
//		ejemploIntervalInfinito2();
//		ejemploIntervalInfinitoRetry();
//		ejemploIntervalDesdeCreate();
		ejemploContraPresion();
//		ejemploContraPresion2();
	}
	
	public void ejemplosVarios()throws Exception {
		Flux<String> nombres = Flux.just("Andres", "Pedro", "Diego", "Juan")
				.doOnNext(nombre -> System.out.println(nombre));
		
		//lo anterior se puede hacer de forma resumida de la siguiente forma, que se llama calable
		Flux<String> nombres2 = Flux.just("Jose", "Maria", "Doris", "Teresa")
				.doOnNext(System.out::println);
		
		//Tenemos que suscribirnos al flujo u observable, porque a pesar de estar programado
		//si no nos suscribimos no pasa nada
		//Cuando nos suscribimos estamos observando, por lo tanto se trata de un observador, un consumidor
		//que ejuectua un tipo de tarea consumiendo cada elemento que esta emitiendo el observable
		//vamos a interrumpir cuando llegue i a 5
		//no solo lo puede consumir sino manejar cualquier tipo de error que pueda ocurrir
		
//		nombres.subscribe();
//		nombres2.subscribe(log::info);
		//Lo anterior tambien se puede hacer asi con lambda
		//nombres2.subscribe(e-> log.info(e));
		
		Flux<String> nombres3 = Flux.just("Tomas", "", "Luis")
				.doOnNext( x ->{
					if ( x.isEmpty()) {
						throw new RuntimeException("Nombre no puede ser vacio");
					} 
						System.out.println(x);
					
				});
		//primer parametro, vamos a consumir, y el segundo para manejar el error
//		nombres3.subscribe(e-> log.info(e), 
//				error -> log.error(error.getMessage()));
		
		//Hay otra forma del metdo subscribe, que nos permite hacer una tarea cuando
		//finaliza o termina la suscripcion es decir cuando termina  de emitir el ultimo elemento el flux
		//se hay un error no se ejecuta el runnable
//		nombres2.subscribe(e-> log.info(e), 
//				error -> log.error(error.getMessage()),
//				new Runnable() {
//					
//					@Override
//					public void run() {
//						log.info("Ha finalizado la ejecución del observable con éxito");
//						
//					}
//				});
		
		//OPERADOR MAP
		Flux<String> nombres5 = Flux.just("nombre1", "nombre2", "nombre3")
				.map(nombre -> {
					return nombre.toUpperCase();
					})
				.doOnNext( x ->{
					if ( x.isEmpty()) {
						throw new RuntimeException("Nombre no puede ser vacio");
					} 
					System.out.println("hola"+x);
				})
				.map( s -> {
					return s.toLowerCase();
				});
		
//		nombres5.subscribe(e-> log.info(e), 
//				error -> log.error(error.getMessage()),
//				new Runnable() {
//					
//					@Override
//					public void run() {
//						log.info("Ha finalizado la ejecución del observable con éxito");
//						
//					}
//				});
		
		Flux<Usuario> nombres6 = Flux.just("nombre1", "nombre2", "nombre3")
				.map(nombre -> new Usuario(nombre.toUpperCase(), null))
				.doOnNext( usuario ->{
					if ( usuario==null ) {
						throw new RuntimeException("Nombre no puede ser vacio");
					} 
					System.out.println(usuario.getNombre());
				})
				.map( usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				});
		
		
//		nombres6.subscribe(e-> log.info(e.toString()), 
//		error -> log.error(error.getMessage()),
//		new Runnable() {
//			
//			@Override
//			public void run() {
//				log.info("Ha finalizado la ejecución del observable con éxito");
//				
//			}
//		});
		
//OPERADOR FILTER
		
		Flux<Usuario> nombres7 = Flux.just("Andres Guzman", "Pedro Fulano", "Maria Fulana", "Diego Sultano", "Juan Mengano", "Bruce Lee", "Bruce Willis")
				.map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))
				.filter (usuario -> usuario.getNombre().equalsIgnoreCase("Bruce"))
				.doOnNext( usuario ->{
					if ( usuario==null ) {
						throw new RuntimeException("Nombre no puede ser vacio");
					} 
					System.out.println(usuario.getNombre().concat(" ").concat(usuario.getApellido()));
				})
				.map( usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					usuario.setApellido(usuario.getApellido().toLowerCase());
					return usuario;
				});
		
		
//		nombres7.subscribe(e-> log.info(e.toString()), 
//		error -> log.error(error.getMessage()),
//		new Runnable() {
//			
//			@Override
//			public void run() {
//				log.info("Ha finalizado la ejecución del observable con éxito");
//				
//			}
//		});

//INMUTABILIDAD		
		
		Flux<String> nombres8 = Flux.just("Andres Guzman", "Pedro Fulano", "Maria Fulana", "Diego Sultano", "Juan Mengano", "Bruce Lee", "Bruce Willis");
				
		Flux<Usuario> usuarios = nombres8.map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))
				.filter (usuario -> usuario.getNombre().equalsIgnoreCase("Bruce"))
				.doOnNext( usuario ->{
					if ( usuario==null ) {
						throw new RuntimeException("Nombre no puede ser vacio");
					} 
					System.out.println(usuario.getNombre().concat(" ").concat(usuario.getApellido()));
				})
				.map( usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					usuario.setApellido(usuario.getApellido().toLowerCase());
					return usuario;
				});
		
		
//		usuarios.subscribe(e-> log.info(e.toString()), 
//		error -> log.error(error.getMessage()),
//		new Runnable() {
//			
//			@Override
//			public void run() {
//				log.info("Ha finalizado la ejecución del observable con éxito");
//				
//			}
//		});

	
	//LIST O ITERABLE
	
	List<String> usuariosList = Arrays.asList(new String[]{"Andres Guzman", "Pedro Fulano", "Maria Fulana", "Diego Sultano", "Juan Mengano", "Bruce Lee", "Bruce Willis"});

	//Stream reactivo
	Flux<String> flujoUsuarios = Flux.fromIterable(usuariosList);
	Flux<Usuario> usuariosFlujo = flujoUsuarios.map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))
			.filter (usuario -> usuario.getNombre().equalsIgnoreCase("Bruce"))
			.doOnNext( usuario ->{
				if ( usuario==null ) {
					throw new RuntimeException("Nombre no puede ser vacio");
				} 
				System.out.println(usuario.getNombre().concat(" ").concat(usuario.getApellido()));
			})
			.map( usuario -> {
				String nombre = usuario.getNombre().toLowerCase();
				usuario.setNombre(nombre);
				usuario.setApellido(usuario.getApellido().toLowerCase());
				return usuario;
			});
	
	
		usuariosFlujo.subscribe(e-> log.info(e.toString()), 
		error -> log.error(error.getMessage()),
		new Runnable() {
			
			@Override
			public void run() {
				log.info("Ha finalizado la ejecución del observable con éxito");
				
			}
		});
	}
	
	public void ejemploFlatMap()throws Exception {
		
		//flatMap convierte a otro tipo de flujo mono o flux
		
		List<String> usuariosList = Arrays.asList(new String[]{"Andres Guzman", "Pedro Fulano", "Maria Fulana", "Diego Sultano", "Juan Mengano", "Bruce Lee", "Bruce Willis"});

		//Stream reactivo
				 Flux.fromIterable(usuariosList).map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))
				.flatMap (usuario -> {
					if( usuario.getNombre().equalsIgnoreCase("bruce")) {
						return Mono.just(usuario);  //esto es un observable
					} else {
						return Mono.empty();
					} 
				})
				.map( usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					usuario.setApellido(usuario.getApellido().toLowerCase());
					return usuario;
				})
				.subscribe(u-> log.info(u.toString()) 
						
				);
		
		
			
	}
	
	public void ejemploToString()throws Exception {
		
		//flatMap convierte a otro tipo de flujo mono o flux
		
		List<Usuario> usuariosList = new ArrayList<>();
		usuariosList.add(new Usuario("Andres", "Guzman"));
		usuariosList.add(new Usuario("Pedro", "Fulano"));
		usuariosList.add(new Usuario("Maria", "Fulana"));
		usuariosList.add(new Usuario("Diego", "Sultano"));
		usuariosList.add(new Usuario("Juan", "Mengano"));
		usuariosList.add(new Usuario("Bruce", "Lee"));
		usuariosList.add(new Usuario("Bruce", "Willis"));
			

		//Stream reactivo
				 Flux.fromIterable(usuariosList)
				 .map(usuario -> usuario.getNombre().toUpperCase().concat(" ").concat(usuario.getApellido().toLowerCase()))
				.flatMap (x -> {
					if( x.contains("bruce".toUpperCase())) {
						return Mono.just(x);  //esto es un observable
					} else {
						return Mono.empty();
					} 
				})
				.map( usuario -> {

					return usuario.toLowerCase();
				})
				.subscribe(u-> log.info(u.toString()) 
						
				);
		
		
			
	}

	public void ejemploCollectList()throws Exception {
		
		//flatMap convierte a otro tipo de flujo mono o flux
		
		List<Usuario> usuariosList = new ArrayList<>();
		usuariosList.add(new Usuario("Andres", "Guzman"));
		usuariosList.add(new Usuario("Pedro", "Fulano"));
		usuariosList.add(new Usuario("Maria", "Fulana"));
		usuariosList.add(new Usuario("Diego", "Sultano"));
		usuariosList.add(new Usuario("Juan", "Mengano"));
		usuariosList.add(new Usuario("Bruce", "Lee"));
		usuariosList.add(new Usuario("Bruce", "Willis"));
			

		//collectList convierte a un mono que contiene un solo objeto que seria la lista de usuarios
				 Flux.fromIterable(usuariosList)
				 .collectList()
				 .subscribe( listaUsuarios -> log.info(listaUsuarios.toString()));
			
	}
	
	public void ejemploCollectList2()throws Exception {
		
		//flatMap convierte a otro tipo de flujo mono o flux
		
		List<Usuario> usuariosList = new ArrayList<>();
		usuariosList.add(new Usuario("Andres", "Guzman"));
		usuariosList.add(new Usuario("Pedro", "Fulano"));
		usuariosList.add(new Usuario("Maria", "Fulana"));
		usuariosList.add(new Usuario("Diego", "Sultano"));
		usuariosList.add(new Usuario("Juan", "Mengano"));
		usuariosList.add(new Usuario("Bruce", "Lee"));
		usuariosList.add(new Usuario("Bruce", "Willis"));
			

		//collectList convierte a un mono que contiene un solo objeto que seria la lista de usuarios
				 Flux.fromIterable(usuariosList)
				 .collectList()
				 .subscribe( listaUsuarios -> {
					 listaUsuarios.forEach( item -> log.info( item.toString()));
					});
			
	}
	
	private Usuario crearUsuario(String nombre, String apellido) {
		return new Usuario(nombre, apellido) ;
	}
	

	
	//Combina dos flujos con el operador flatMap
	public void ejemploUsuarioComentariosFlatMap()throws Exception {

		//forma 1
		Mono<Usuario> usuarioMono = Mono.fromCallable(()-> crearUsuario("John", "Doe"));
		
		Mono<Comentarios> comentariosMono = Mono.fromCallable(()-> {
			Comentarios comentarios =  new Comentarios();
			comentarios.addComentario("comentario1");
			comentarios.addComentario("comentario2");
			comentarios.addComentario("comentario3");
			return comentarios;
		});
		
		usuarioMono.flatMap( u -> comentariosMono.map(c -> new UsuarioComentarios(u, c)))
		.subscribe(uc -> log.info(uc.toString()));
	}
	
	// Combinaar dos flujos con el operador zipWith
	public void ejemploUsuarioComentariosZipWith()throws Exception {
		
		
		//forma 2 o al vuelo
		
		Mono<Usuario> usuarioMono = Mono.fromCallable(()-> {
			return new Usuario("John", "Doe");
		});
		
		Mono<Comentarios> comentariosMono = Mono.fromCallable(()-> {
			Comentarios comentarios =  new Comentarios();
			comentarios.addComentario("comentario1");
			comentarios.addComentario("comentario2");
			comentarios.addComentario("comentario3");
			return comentarios;
		});
		//primero
		usuarioMono.zipWith(comentariosMono, (u,c) -> new UsuarioComentarios(u, c))
		.subscribe(uc -> log.info(uc.toString()));
	}
	
	public void ejemploUsuarioComentariosZipWith2()throws Exception {
		
		
		//forma 2 o al vuelo
		
		Mono<Usuario> usuarioMono = Mono.fromCallable(()-> {
			return new Usuario("John", "Doe");
		});
		
		Mono<Comentarios> comentariosMono = Mono.fromCallable(()-> {
			Comentarios comentarios =  new Comentarios();
			comentarios.addComentario("comentario1");
			comentarios.addComentario("comentario2");
			comentarios.addComentario("comentario3");
			return comentarios;
		});
		//primero
		Mono<UsuarioComentarios> monoUsuarioComentario = usuarioMono.zipWith(comentariosMono, (u,c) -> new UsuarioComentarios(u, c));
		
		monoUsuarioComentario.subscribe(uc -> log.info(uc.toString()));
	}
	public void ejemploUsuarioComentariosZipWithForma2()throws Exception {
		
		
		//forma 2 o al vuelo
		
		Mono<Usuario> usuarioMono = Mono.fromCallable(()-> {
			return new Usuario("John", "Doe");
		});
		
		Mono<Comentarios> comentariosMono = Mono.fromCallable(()-> {
			Comentarios comentarios =  new Comentarios();
			comentarios.addComentario("comentario1");
			comentarios.addComentario("comentario2");
			comentarios.addComentario("comentario3");
			return comentarios;
		});
		//primero
		Mono<UsuarioComentarios> monoUsuarioComentario = usuarioMono
				.zipWith(comentariosMono)
				.map( tuple ->{
					Usuario u = tuple.getT1();
					Comentarios c = tuple.getT2();
					return new UsuarioComentarios(u, c);
				});
		
		monoUsuarioComentario.subscribe(uc -> log.info(uc.toString()));
	}
	
	public void ejemploZipWithRangos()throws Exception {
		
		Flux.just(1 ,2 ,3 ,4 )
		.map( i -> (i*2) )
		.zipWith(Flux.range(0, 4), ( uno, dos )-> String.format("Primer flux: %d, Segundo flux: %d", uno, dos))
		.subscribe(texto -> log.info(texto));
	}
	
	public void ejemploZipWithRangos2()throws Exception {
		
		Flux<Integer> rango =Flux.range(0, 4);
		Flux.just(1 ,2 ,3 ,4 )
		.map( i -> (i*2) )
		.zipWith(rango, ( uno, dos )-> String.format("Primer flux: %d, Segundo flux: %d", uno, dos))
		.subscribe(texto -> log.info(texto));
	}
	//doOnNext es para aplicar una tarea
	//esto no se veria por consola, porque termina el main y este flujo u observable se sigue ejecutando en segundo plano
	//o en paralelo en la maquina virtual de java, y esto es por el retraso o delay por 12 segundos
	//para ver esto por consola tendriamos que forzar el bloque
	public void ejemploInterval()throws Exception {
		Flux<Integer> rango = Flux.range(1, 12);
		Flux<Long> retraso = Flux.interval(Duration.ofSeconds(1));
		rango.zipWith(retraso, (ra, re) -> ra)
		.doOnNext( i -> log.info( i.toString()))
		.subscribe();
	}
	//vamos a forzar el bloqueo (hasta el ultimo elemento)
	//lo que hace es subscribir a este flujo pero con bloqueo
	public void ejemploIntervalConBloqueo()throws Exception {
		Flux<Integer> rango = Flux.range(1, 12);
		Flux<Long> retraso = Flux.interval(Duration.ofSeconds(1));
		rango.zipWith(retraso, (ra, re) -> ra)
		.doOnNext( i -> log.info( i.toString()))
		.blockLast();
	}
	
	public void ejemploDelayElements()throws Exception {
		Flux<Integer> rango = Flux.range(1, 12);
		rango.delayElements(Duration.ofSeconds(1))
		.doOnNext(i -> log.info( i.toString()))
		.blockLast();
	}
	
	public void ejemploDelayElements2()throws Exception {
		Flux<Integer> rango = Flux.range(1, 12);
		rango.delayElements(Duration.ofSeconds(1))
		.doOnNext(i -> log.info( i.toString()))
		.subscribe();
		
		Thread.sleep(13000);
	}
	//vamos a bloquear de otra forma
	// comienza en uno y se va a decrementar
	//cuando llegue a cero va a liberar el thread
	//se decrementa en el evento doOnTerminate
	public void ejemploIntervalInfinito()throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		
		Flux.interval(Duration.ofSeconds(1))
		.doOnTerminate(()-> latch.countDown())
		.map(i -> String.valueOf(i))
		.doOnNext( s -> log.info(s))
		.subscribe();
		

		
		
		//vamos a dejar esperando el thread hasta que el contador llegue a cero
		latch.await();
		
	}
	
	public void ejemploIntervalInfinito2()throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		
		Flux.interval(Duration.ofSeconds(1))
		.doOnTerminate(latch::countDown)
		.flatMap( i ->{
			if( i>=5) {
				return Flux.error( new InterruptedException("Solo hasta 5"));
			}
			return Flux.just(i);
		})
		.map(i -> String.valueOf(i))
		.subscribe(s -> log.info(s), e-> log.error(e.getMessage()));
		

		
		
		//vamos a dejar esperando el thread hasta que el contador llegue a cero
		latch.await();
		
	}
	

	//operador retry, va a intentar que cada vez que falle, ejecutar el flujo n cantidad de veces
	//se va a ejecutar tres veces, la primera y luego dos del retry
	public void ejemploIntervalInfinitoRetry()throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		
		Flux.interval(Duration.ofSeconds(1))
		.doOnTerminate(latch::countDown)
		.flatMap( i ->{
			if( i>=5) {
				return Flux.error( new InterruptedException("Solo hasta 5"));
			}
			return Flux.just(i);
		})
		.map(i -> String.valueOf(i))
		.retry(2)
		.subscribe(s -> log.info(s), e-> log.error(e.getMessage()));
		

		
		
		//vamos a dejar esperando el thread hasta que el contador llegue a cero
		latch.await();
		
	}
	
	//Operador Create de flux
	public void ejemploIntervalDesdeCreate()throws Exception {
		
		Flux.create(emitter -> {
			Timer timer = new Timer();
			timer.schedule(new TimerTask() {
				
				private Integer contador =0;
				@Override
				public void run() {
					// TODO Auto-generated method stub
					emitter.next(++contador);
					if(contador == 10) {
						timer.cancel();
						emitter.complete();
						
					}
					
					if(contador == 5) {
						timer.cancel();
						emitter.error( new InterruptedException("Error, se ha detenido el flux en 5"));
						
					}
				}
			}, 1000, 1000);
		})

		.subscribe(next -> log.info(String.valueOf(next)), 	
				error -> log.error(error.getMessage()),
				()-> log.info("Hemos terminado"));
		
		
		
	}
	
	//Backpressure: Es cuando el flujo de abajo el suscriptor, le puede pedir al flujo 
	//de arriba el productor, que envie menos datos o de a poco para evitar la sobrecarga
	//request es donde se dice la cantidad de elementos a recibir
	public void ejemploContraPresion()throws Exception {
//		Flux.range(1, 10)
//		.log()
//		.subscribe(i-> log.info(i.toString()));
		
		Flux.range(1, 10)
		.log()
		.subscribe( new Subscriber<Integer>() {
			
			private Subscription s;
			private Integer limite = 5;
			private Integer consumido = 0;

			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				s.request(limite);
				
			}

			@Override
			public void onNext(Integer t) {
				log.info(t.toString());
				consumido++;
				if ( consumido == limite ) {
					consumido = 0;
					s.request(limite);
				}
				
			}

			@Override
			public void onError(Throwable t) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onComplete() {
				// TODO Auto-generated method stub
				
			}
		});
	}
	
	public void ejemploContraPresion2()throws Exception {
//		Flux.range(1, 10)
//		.log()
//		.subscribe(i-> log.info(i.toString()));
		
		Flux.range(1, 10)
		.log()
		.limitRate(2)
		.subscribe( );
	}
}
