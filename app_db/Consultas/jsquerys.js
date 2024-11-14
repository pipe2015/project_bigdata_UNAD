var run_querys = () => {
    //Seleccionar todos los usuarios:
    print("\n1. Seleccionar todos los usuarios\n")
    db.user.find({})

    //Seleccionar todas las categorias por articulos
    var all_category_article = () => {
        print("\n2. Seleccionar todas las categorias por articulos\n")
        return db.category.aggregate([
            {
                $lookup: {
                    from: "article", // Colecccion con la que se hace la relacion
                    localField: "_id", // Campo en Category que se usara para la relación
                    foreignField: "category", // Campo en Article que se usara para la relación
                    as: "articles" // Nombre del array de resultados
                }
            },
            {
                $project: { //pasamos los datos retornados por la $lookup en as
                    category_name: 1, // pasamos el dato, nombre de la categoria
                    articles: { 
                        /* modificamos los datos con la funcion $map le pasamos el array (articles) lo nombramos con un alias(article)
                        y retornamos los articulos por cada categoria 
                        */
                        $map: { input: "$articles", as: "article", in: "$$article.article_name" } 
                    }
                }
            }
        ]).map(result => {
            articles = result.articles.reduce((str_concat, v) => `${str_concat}${v}\n`, '')
            return `Category: ${result.category_name}, Articles: ${articles}`;
        });
    }

    //Seleccionar todas la sessiones por un usuario de nombre
    var get_sessions_oneuser = user_name => {
        //imprimimos el valos de los datos
        print(`\n3. Sesiones para el usuario '${user_name}':\n`);
    
        return db.user.aggregate([
            {
                $match: { name: user_name } // Filtra el usuario por nombre
            },
            {
                $lookup: {
                    from: "session",         // Colección relacionada
                    localField: "_id",        // Campo en User (clave primaria)
                    foreignField: "user",     // Campo en Session que referencia al usuario
                    as: "sessions"            // Nombre del campo donde se guardarán las sesiones
                }
            },
            { $unwind: "$sessions" },          // separamos los datos del array y los obtenemos en un solo documento cada uno (json)
            {
                $project: {
                    "sessions.session_id": 1, // require 
                    "sessions.timestamp": 1, // require 
                    "sessions.browser_type": 1, // require 
                    "sessions.device_type": 1 // require 
                }
            }
        ]).map(result => {
            let session = result.sessions;
            return `Session ID: ${session.session_id}, Tiempo de Session: ${session.timestamp}, Navegador: ${session.browser_type}, Dispositivo: ${session.device_type}`
        });
    }
    
    

    all_category_article().pretty()
    get_sessions_oneuser('David Lopez').pretty()
}
