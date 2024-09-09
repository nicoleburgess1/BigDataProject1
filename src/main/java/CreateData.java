import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.Random;

public class CreateData {
    public static void addAllData(String[] args) {
        File data = new File("data.csv");
        Random rand = new Random();

        try (PrintWriter writer = new PrintWriter(new FileWriter(data))) {
            writer.println("ID,Nickname,Occupation,NCode,HighestEdu");

            for(int i=1; i<=200000; i++){
                int nicknamesList = 0;
                int id = i;
                String nickname = "";
                String occupation = "";
                int nCode = rand.nextInt(50) + 1;;
                String highestEdu = "";
                writer.println(id + "," + nickname + "," + occupation + "," + nCode + "," + highestEdu);
                nicknamesList++;
            }
            System.out.println("CSV file written successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing the CSV file.");
            e.printStackTrace();
        }
    }
}
