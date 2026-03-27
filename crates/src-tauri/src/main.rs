// On importe une fonction de votre projet d'origine
use oxide_gui::lancement_interface as ma_fonction_super_utile ;

#[tauri::command]
fn appeler_mon_code() -> String {
    // On exécute votre logique métier ici !
    ma_fonction_super_utile();
    "Logique exécutée avec succès !".to_string()
}

fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![appeler_mon_code])
        .run(tauri::generate_context!())
        .expect("Erreur lors du lancement");
}