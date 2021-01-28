package com.bolsadeideas.springboot.reactor.app.models;

import java.util.ArrayList;
import java.util.List;

public class Comentarios {

	private List<String> ListaComentarios;

	public Comentarios() {
		super();
		this.ListaComentarios = new ArrayList<>();
	}

	public void addComentario(String comentario) {
		this.ListaComentarios.add(comentario);
	}

	@Override
	public String toString() {
		return "comentarios=" + ListaComentarios;
	}

	
	
	
	
	
}
