

class PHTManager {

    public static void main(String args[]) {
        if (args.length != 2){
            System.out.println("Please use this format: \njava PHTManager test.sqlite instructions.txt");
            System.exit(1);
        }

        Database db = new Database(args[0], args[1]);
        db.initial_tables();
        db.parse_instructions(args[1]);

    }


}
