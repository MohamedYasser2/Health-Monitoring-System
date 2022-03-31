package com.healthmonitor.healthmonitorbackend;

import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;

@RestController
@RequestMapping("/api")
@CrossOrigin
public class HomeController {
    @GetMapping("/getstatistics")
    public ArrayList<String> getStatistics(@RequestParam String startDate, @RequestParam String endDate) {
        System.out.println("I am hereeeeeee");
        ArrayList<String> test=new ArrayList<>();
        test.add(startDate);
        test.add(endDate);
        return test;
    }
}
//